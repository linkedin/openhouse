package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * Runs Iceberg table maintenance for the VACUUM command as thin sugar over the catalog's stored
 * procedures. VACUUM is an '''Alpha''' feature and is opt-in per table via the
 * `vacuum.enabled` property; it is rejected on tables that have not enabled it. It is also
 * rejected on tables with HCR enabled (`retention.backup.enabled=true`), because VACUUM deletes
 * files directly instead of moving them to the table's backup directory.
 *
 * When `REMOVE ORPHAN FILES` is given, orphan-file deletion runs first (it only removes
 * unreferenced files from storage, so it works even when the table is out of quota, unlike snapshot
 * expiration which commits metadata); snapshot expiration always runs afterwards. Both procedures
 * run with their default file-cleaning behavior, so VACUUM deletes the reclaimed files.
 *
 * The retention window comes from `RETAIN n HOURS` when given. When it is omitted, the cutoffs are
 * derived from the table's OpenHouse properties rather than the Iceberg procedure defaults, and
 * applied the same way the scheduled maintenance jobs apply them. Snapshot expiration uses the
 * history policy (the `policies` property's `history` block): first it expires snapshots older than
 * maxAge x granularity, then, when versions is set and more than versions snapshots remain, it
 * keeps only the last versions snapshots. Orphan-file deletion uses `ofd.one_day_ttl.enabled`
 * (1 day when enabled, otherwise the 3-day default).
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
        if (VacuumTableExec.isHcrEnabled(iceberg.table().properties())) {
          throw new UnsupportedOperationException(
            s"VACUUM is not supported on tables with HCR enabled " +
              s"('${VacuumTableExec.HCR_ENABLED_PROP}' = 'true').")
        }
        // VACUUM is an Alpha feature and is opt-in per table: a table must explicitly enable it via
        // the `vacuum.enabled` property, otherwise the command is rejected.
        if (!"true".equalsIgnoreCase(iceberg.table().properties().get(VacuumTableExec.ENABLED_PROP))) {
          throw new UnsupportedOperationException(
            s"VACUUM is an Alpha feature and must be enabled on the table before use. Enable it " +
              s"with: ALTER TABLE <table> SET TBLPROPERTIES ('${VacuumTableExec.ENABLED_PROP}' = 'true').")
        }
        val quotedCatalog = quoteIfNeeded(catalog.name())
        val tableArg = (ident.namespace() :+ ident.name()).map(quoteIfNeeded).mkString(".")
        val props = iceberg.table().properties()
        val now = Instant.now()

        // Procedure arguments must be foldable, so a retention window is resolved here to a literal
        // `older_than` timestamp rather than a `current_timestamp()` expression. The literal is
        // rendered in the session time zone because the CALL's `TIMESTAMP '...'` literal is parsed
        // back in that same zone, so the round-trip preserves the intended instant.
        def olderThanClause(instant: Instant): String = {
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.of(spark.sessionState.conf.sessionLocalTimeZone))
          s", older_than => TIMESTAMP '${formatter.format(instant)}'"
        }

        if (removeOrphanFiles) {
          // Runs BEFORE expiration. Snapshot expiration commits table metadata, so it cannot run on
          // a table that is out of quota; orphan-file deletion only removes unreferenced files from
          // storage and always can, so doing it first ensures it still runs in that case. Running
          // first also means it scans against the pre-expiration referenced-file set, so it can
          // never delete a file that a still-live snapshot references.
          //
          // Cutoff: an explicit RETAIN overrides; otherwise honor the OpenHouse OFD TTL property
          // (1 day when ofd.one_day_ttl.enabled=true, else the 3-day default).
          val ofdOlderThan = retainHours match {
            case Some(hours) => olderThanClause(now.minus(hours.toLong, ChronoUnit.HOURS))
            case None =>
              olderThanClause(now.minus(VacuumTableExec.ofdRetainDays(props), ChronoUnit.DAYS))
          }
          spark.sql(
            s"CALL $quotedCatalog.system.remove_orphan_files(table => '$tableArg'$ofdOlderThan)").collect()
        }

        def expireSnapshots(args: String): Unit =
          spark.sql(s"CALL $quotedCatalog.system.expire_snapshots(table => '$tableArg'$args)").collect()

        // Snapshot expiration always runs and cleans reclaimed files. Cutoff: an explicit RETAIN
        // overrides; otherwise apply the OpenHouse history policy as the scheduled expiration job
        // does, in two phases: expire by age, then cap the remaining snapshots at versions.
        retainHours match {
          case Some(hours) => expireSnapshots(olderThanClause(now.minus(hours.toLong, ChronoUnit.HOURS)))
          case None =>
            val history = VacuumTableExec.parseHistoryRetention(props.get(VacuumTableExec.POLICIES_PROP))
            val ageMillis = VacuumTableExec.granularityToChrono(history.granularity)
              .getDuration.multipliedBy(history.maxAge.toLong).toMillis
            expireSnapshots(olderThanClause(now.minusMillis(ageMillis)))

            if (history.versions > 0) {
              val table = iceberg.table()
              table.refresh()
              if (table.snapshots().asScala.size > history.versions) {
                expireSnapshots(olderThanClause(Instant.now()) + s", retain_last => ${history.versions}")
              }
            }
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

object VacuumTableExec {
  /**
   * Table property that opts a table into the Alpha VACUUM command. VACUUM throws
   * [[UnsupportedOperationException]] on tables where this is not set to `true`.
   */
  val ENABLED_PROP = "vacuum.enabled"

  /** Table property that enables HCR (retention backup). VACUUM rejects tables where it is `true`. */
  val HCR_ENABLED_PROP = "retention.backup.enabled"

  /** OpenHouse table property holding the policies JSON (retention, history, etc.). */
  val POLICIES_PROP = "policies"

  /** OpenHouse property that opts a table into a 1-day orphan-file-deletion TTL. */
  val OFD_ONE_DAY_TTL_ENABLED_PROP = "ofd.one_day_ttl.enabled"

  /** Default orphan-file-deletion TTL in days when the 1-day opt-in is not set. */
  val DEFAULT_OFD_TTL_DAYS = 3L

  // History-policy defaults, matching OpenHouse's server-side defaults.
  val DEFAULT_HISTORY_MAX_AGE = 3
  val DEFAULT_HISTORY_GRANULARITY = "DAY"
  val DEFAULT_HISTORY_VERSIONS = 0

  private val mapper = new ObjectMapper()

  /** The snapshot-retention values honored by VACUUM, read from the history policy. */
  case class HistoryRetention(maxAge: Int, granularity: String, versions: Int)

  /** True when the table has HCR enabled. Exposed for testing. */
  def isHcrEnabled(props: java.util.Map[String, String]): Boolean =
    "true".equalsIgnoreCase(props.get(HCR_ENABLED_PROP))

  /**
   * Orphan-file-deletion retention in days: 1 when `ofd.one_day_ttl.enabled` is `true`, otherwise
   * the 3-day default. Exposed for testing.
   */
  def ofdRetainDays(props: java.util.Map[String, String]): Long =
    if ("true".equalsIgnoreCase(props.get(OFD_ONE_DAY_TTL_ENABLED_PROP))) 1L else DEFAULT_OFD_TTL_DAYS

  /**
   * Parse the history block of the OpenHouse `policies` JSON into the snapshot-retention values.
   * Absent property, absent history block, or unparseable JSON all fall back to the OpenHouse
   * defaults (maxAge=3, granularity=DAY, versions=0). Exposed for testing.
   */
  def parseHistoryRetention(policiesJson: String): HistoryRetention = {
    val default = HistoryRetention(
      DEFAULT_HISTORY_MAX_AGE, DEFAULT_HISTORY_GRANULARITY, DEFAULT_HISTORY_VERSIONS)
    if (policiesJson == null || policiesJson.trim.isEmpty) return default
    try {
      val history = mapper.readTree(policiesJson).path("history")
      if (history.isMissingNode || history.isNull) return default
      HistoryRetention(
        maxAge = if (history.hasNonNull("maxAge")) history.get("maxAge").asInt(DEFAULT_HISTORY_MAX_AGE)
          else DEFAULT_HISTORY_MAX_AGE,
        granularity = if (history.hasNonNull("granularity")) history.get("granularity").asText(DEFAULT_HISTORY_GRANULARITY)
          else DEFAULT_HISTORY_GRANULARITY,
        versions = if (history.hasNonNull("versions")) history.get("versions").asInt(DEFAULT_HISTORY_VERSIONS)
          else DEFAULT_HISTORY_VERSIONS)
    } catch {
      case _: Exception => default
    }
  }

  /**
   * Convert an OpenHouse history-policy granularity to the ChronoUnit used to compute the age cutoff,
   * matching the OpenHouse expiration job. Unknown values fall back to DAYS. Exposed for testing.
   */
  def granularityToChrono(granularity: String): ChronoUnit =
    granularity.toUpperCase match {
      case "HOUR" => ChronoUnit.HOURS
      case "DAY" => ChronoUnit.DAYS
      case "MONTH" => ChronoUnit.MONTHS
      case "YEAR" => ChronoUnit.YEARS
      case _ => ChronoUnit.DAYS
    }
}
