package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * Runs Iceberg table maintenance for the VACUUM command as thin sugar over the catalog's stored
 * procedures. Snapshot expiration always runs; when `REMOVE ORPHAN FILES` is given, orphan-file
 * deletion runs afterwards so it cleans up against the settled live-file set. A `RETAIN n HOURS`
 * window bounds both operations via the procedures' `older_than` argument; when omitted, each
 * procedure falls back to its own default retention.
 */
case class VacuumTableExec(
  spark: SparkSession,
  catalog: TableCatalog,
  ident: Identifier,
  removeOrphanFiles: Boolean,
  retainHours: Option[Int]) extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable if iceberg.table().properties().containsKey("openhouse.tableId") =>
        val quotedCatalog = quoteIfNeeded(catalog.name())
        val tableArg = (ident.namespace() :+ ident.name()).map(quoteIfNeeded).mkString(".")

        // Procedure arguments must be foldable, so a RETAIN window is resolved here to a literal
        // `older_than` timestamp (now - n hours) rather than a `current_timestamp()` expression.
        // The literal is rendered in the session time zone because the CALL's `TIMESTAMP '...'`
        // literal is parsed back in that same zone, so the round-trip preserves the intended
        // instant. When RETAIN is omitted, `older_than` is left off so each procedure applies its
        // own default retention.
        val olderThanArg = retainHours.map { hours =>
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.of(spark.sessionState.conf.sessionLocalTimeZone))
          val cutoff = formatter.format(Instant.now().minus(hours.toLong, ChronoUnit.HOURS))
          s", older_than => TIMESTAMP '$cutoff'"
        }.getOrElse("")

        // Snapshot expiration always runs.
        spark.sql(
          s"CALL $quotedCatalog.system.expire_snapshots(table => '$tableArg'$olderThanArg)").collect()

        if (removeOrphanFiles) {
          // Runs after expiration so it deletes against the settled live-file set.
          spark.sql(
            s"CALL $quotedCatalog.system.remove_orphan_files(table => '$tableArg'$olderThanArg)").collect()
        }

      case table =>
        throw new UnsupportedOperationException(s"Cannot vacuum non-Openhouse table: $table")
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"VacuumTableExec: ${catalog} ${ident} removeOrphanFiles=${removeOrphanFiles} " +
      s"retainHours=${retainHours.getOrElse("default")}"
  }
}
