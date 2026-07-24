package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import java.util.Locale

import scala.collection.JavaConverters._

import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.OptimizeTable
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableChange}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec
import org.apache.spark.sql.functions.{col, current_timestamp, expr, lit, max}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Runs Iceberg data-layout maintenance for the OPTIMIZE command as thin sugar over the catalog's
 * stored procedures. With no `optimize.cluster.keys` configured this is a plain bin-pack compaction
 * (`system.rewrite_data_files`); with clustering configured it is a sort / z-order rewrite that is
 * incremental by default (only the forward slice of the leading key since the last run, tracked by
 * the `optimize.cluster.hwm-snapshot-id` watermark), with `FULL` reclustering up to the age floor.
 * `REWRITE MANIFESTS` runs afterwards over the post-rewrite layout.
 */
case class OptimizeTableExec(
  output: Seq[Attribute],
  spark: SparkSession,
  catalog: TableCatalog,
  ident: Identifier,
  full: Boolean,
  rewriteManifests: Boolean) extends LeafV2CommandExec {

  private def row(metric: String, value: String): InternalRow =
    new GenericInternalRow(
      Array[Any](UTF8String.fromString(metric), UTF8String.fromString(value)))

  override protected def run(): Seq[InternalRow] = {
    val props = catalog.loadTable(ident) match {
      case iceberg: SparkTable if iceberg.table().properties().containsKey("openhouse.tableId") =>
        iceberg.table().properties().asScala.toMap
      case table =>
        throw new UnsupportedOperationException(s"Cannot optimize non-Openhouse table: $table")
    }

    val cat = quoteIfNeeded(catalog.name())
    val tableArg = (ident.namespace() :+ ident.name()).map(quoteIfNeeded).mkString(".")
    val qualifiedTableName = s"$cat.$tableArg"

    // Snapshot the physical layout before doing any work so we can report the reduction.
    val filesBefore = spark.table(s"$qualifiedTableName.files").count()
    val snapshotsBefore = spark.table(s"$qualifiedTableName.snapshots").count()

    val config = OptimizeTable.parseClusterConfig(props)
    config.keys match {
      case Seq() =>
        // No clustering configured: plain bin-pack compaction (unchanged historical behavior).
        spark.sql(s"CALL $cat.system.rewrite_data_files(table => '$tableArg')").collect()
      case _ =>
        cluster(cat, tableArg, qualifiedTableName, config)
    }

    if (rewriteManifests) {
      // Independent manifest compaction; runs after the data rewrite so it sees the new layout.
      spark.sql(s"CALL $cat.system.rewrite_manifests(table => '$tableArg')").collect()
    }

    val filesAfter = spark.table(s"$qualifiedTableName.files").count()
    val snapshotsAfter = spark.table(s"$qualifiedTableName.snapshots").count()
    Seq(
      row("files_before", filesBefore.toString),
      row("files_after", filesAfter.toString),
      row("files_removed", (filesBefore - filesAfter).toString),
      row("snapshots_committed", (snapshotsAfter - snapshotsBefore).toString))
  }

  private def cluster(
      cat: String,
      tableArg: String,
      qualifiedTableName: String,
      config: OptimizeTable.ClusterConfig): Unit = {
    import config.{hwm, keys, maxCommits, minAgeMinutes, sortMode, state}

    // Age floor: the newest snapshot at least `minAgeMinutes` old, by commit time. Everything
    // younger is held back so we never rewrite files a concurrent streaming writer is extending.
    val ageFloor = spark.table(s"$qualifiedTableName.snapshots")
      .where(col("committed_at") <= current_timestamp() - expr(s"INTERVAL $minAgeMinutes MINUTES"))
      .orderBy(col("committed_at").desc)
      .limit(1)
      .select("snapshot_id")
      .collect().headOption.map(_.getLong(0))
    if (ageFloor.isEmpty) return // nothing old enough to consume yet -> no-op

    val floorId = ageFloor.get
    // Incremental run whose watermark has not moved -> nothing new to do.
    if (!full && hwm.contains(floorId)) return

    // The forward slice is bounded on the leading clustering key. Its upper bound is the max value
    // present as of the age floor; Iceberg satisfies this from manifest metrics when the table has
    // no deletes, so it is metadata-only.
    val leadKey = keys.head
    val floorMax = Option(
      spark.read.option("snapshot-id", floorId).table(qualifiedTableName)
        .agg(max(col(quoteIfNeeded(leadKey)))).head().get(0))
    if (floorMax.isEmpty) return // no data as of the age floor -> no-op

    // Lower bound: incremental runs skip what a prior run already clustered -- the last-clustered
    // upper of the current key selection, read from the persisted interval state (a table property,
    // so it survives snapshot expiration). FULL ignores the bound and reclusters everything up to
    // the floor.
    val cfgId = OptimizeTable.configId(keys, sortMode)
    val lowerValue = state.find(_.config == cfgId).map(_.upper).filterNot(_ => full)

    // Cast the persisted (string) bound back to the leading key's type so a key promoted between
    // runs (e.g. INT -> BIGINT) is compared after a cast, not across boxed types.
    val leadType = spark.table(qualifiedTableName).schema(leadKey).dataType
    val lead = col(quoteIfNeeded(leadKey))
    val lowerBound = lowerValue.map(u => lit(u).cast(leadType))

    // No-op if the leading key has not advanced past the last-clustered upper. Evaluate the
    // comparison in Catalyst (not in Scala) so the cast above governs the ordering.
    val advanced = lowerBound.forall { lb =>
      spark.range(1).select(lit(floorMax.get) > lb).head().getBoolean(0)
    }
    if (!advanced) return

    // The `where` slice to recluster: `lead <= floorMax`, plus `lead > lowerBound` for an
    // incremental run. Catalyst renders each key-type literal correctly, then the predicate is
    // embedded as a SQL string literal so its own quotes survive the CALL.
    val scope = lowerBound.map(lb => (lead > lb) && (lead <= lit(floorMax.get)))
      .getOrElse(lead <= lit(floorMax.get)).expr.sql
    val cols = keys.map(quoteIfNeeded).mkString(", ")
    val sortOrder = sortMode.toLowerCase(Locale.ROOT) match {
      case "zorder" => s"zorder($cols)"
      case _ => cols
    }

    // Scoped sort / z-order rewrite with partial progress: min-input-files=1 + rewrite-all=true
    // cluster the region regardless of file count; use-starting-sequence-number keeps concurrent
    // equality-deletes valid. rewrite-all forces a rewrite over the non-empty scope, so a healthy
    // run always commits a snapshot; if none is committed the rewrite failed systemically (partial
    // progress swallows per-group failures), so fail loudly and leave the watermark unadvanced.
    val snapshotsBefore = spark.table(s"$qualifiedTableName.snapshots").count()
    spark.sql(
      s"CALL $cat.system.rewrite_data_files(" +
        s"table => '$tableArg', " +
        "strategy => 'sort', " +
        s"sort_order => '$sortOrder', " +
        s"where => ${Literal(scope).sql}, " +
        "options => map(" +
        "'min-input-files', '1', " +
        "'rewrite-all', 'true', " +
        "'use-starting-sequence-number', 'true', " +
        "'partial-progress.enabled', 'true', " +
        s"'partial-progress.max-commits', '$maxCommits'))").collect()
    if (spark.table(s"$qualifiedTableName.snapshots").count() <= snapshotsBefore) {
      throw new IllegalStateException(
        s"OPTIMIZE clustered no data for '$qualifiedTableName': the scoped rewrite " +
          s"(keys=[${keys.mkString(",")}], sort-mode=$sortMode) committed no snapshot despite a " +
          s"non-empty scope. The watermark was left unadvanced so the run can be retried.")
    }

    // Advance all clustering metadata in one atomic alterTable -- the watermark (the consumed age
    // floor, not head), the config id, and the interval state -- so they never disagree.
    val newState = OptimizeTable.advanceState(
      state, cfgId, keys, sortMode, lowerValue, floorMax.get.toString, full)
    catalog.alterTable(ident,
      TableChange.setProperty(OptimizeTable.HWM_PROP, floorId.toString),
      TableChange.setProperty(OptimizeTable.CONFIG_ID_PROP, cfgId),
      TableChange.setProperty(OptimizeTable.STATE_PROP, OptimizeTable.stateMapper.writeValueAsString(newState)))
  }

  override def simpleString(maxFields: Int): String = {
    s"OptimizeTableExec: ${catalog} ${ident} full=${full} rewriteManifests=${rewriteManifests}"
  }
}
