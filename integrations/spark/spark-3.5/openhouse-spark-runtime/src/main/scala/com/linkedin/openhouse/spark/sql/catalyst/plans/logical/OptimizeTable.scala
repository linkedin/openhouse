package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.zip.CRC32

import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LeafCommand
import org.apache.spark.sql.types.StringType

/**
 * The logical plan of the OPTIMIZE command:
 * {{{
 *   OPTIMIZE multi_part_name [FULL] [REWRITE MANIFESTS]
 * }}}
 *
 * Behavior depends on whether clustering keys are configured via the `optimize.cluster.*` table
 * properties:
 *
 *  - '''No `optimize.cluster.keys`''': plain bin-pack compaction (`system.rewrite_data_files` with
 *    defaults). Historical behavior, unaffected by `FULL`.
 *  - '''Clustering configured''': a sort / z-order rewrite of the configured keys, incremental by
 *    default (only the forward slice of the leading key since the last run, tracked by an
 *    `optimize.cluster.hwm-snapshot-id` watermark); `FULL` reclusters up to the age floor.
 *
 * `REWRITE MANIFESTS` (`system.rewrite_manifests`) is independent and runs after the data rewrite.
 * Snapshot expiration is intentionally not part of OPTIMIZE -- that is the VACUUM command's job.
 */
case class OptimizeTable(tableName: Seq[String], full: Boolean, rewriteManifests: Boolean)
  extends LeafCommand {

  override lazy val output: Seq[Attribute] = Seq(
    AttributeReference("metric", StringType, nullable = false)(),
    AttributeReference("value", StringType, nullable = false)())

  override def simpleString(maxFields: Int): String = {
    s"OptimizeTable: ${tableName} full=${full} rewriteManifests=${rewriteManifests}"
  }
}

object OptimizeTable {

  val KEYS_PROP = "optimize.cluster.keys"
  val SORT_MODE_PROP = "optimize.cluster.sort-mode"
  val MIN_SNAPSHOT_AGE_PROP = "optimize.cluster.min-snapshot-age-minutes"
  val HWM_PROP = "optimize.cluster.hwm-snapshot-id"
  val MAX_COMMITS_PROP = "optimize.cluster.max-commits"
  val CONFIG_ID_PROP = "optimize.cluster.config-id"
  val STATE_PROP = "optimize.cluster.state"

  val DEFAULT_SORT_MODE = "zorder"
  val DEFAULT_MIN_SNAPSHOT_AGE_MINUTES = 30L
  val DEFAULT_MAX_COMMITS = 10L

  /**
   * Clustering configuration resolved from the `optimize.cluster.*` table properties, with every
   * default applied and every value parsed to its type.
   */
  case class ClusterConfig(
      keys: Seq[String],
      sortMode: String,
      minAgeMinutes: Long,
      maxCommits: Long,
      hwm: Option[Long],
      state: Seq[ClusterInterval])

  /** Parse the `optimize.cluster.*` table properties into a typed [[ClusterConfig]]. */
  def parseClusterConfig(props: Map[String, String]): ClusterConfig = ClusterConfig(
    keys = props.get(KEYS_PROP)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Seq.empty),
    sortMode = props.getOrElse(SORT_MODE_PROP, DEFAULT_SORT_MODE),
    minAgeMinutes = props.get(MIN_SNAPSHOT_AGE_PROP).map(_.toLong)
      .getOrElse(DEFAULT_MIN_SNAPSHOT_AGE_MINUTES),
    maxCommits = props.get(MAX_COMMITS_PROP).map(_.toLong).getOrElse(DEFAULT_MAX_COMMITS),
    hwm = props.get(HWM_PROP).map(_.toLong),
    state = parseState(props.getOrElse(STATE_PROP, "")))

  val stateMapper = {
    val mapper = new ObjectMapper() with ClassTagExtensions
    mapper.registerModule(DefaultScalaModule)
    // Omit an absent `lower` (None) so the persisted JSON stays compact and stable.
    mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
    mapper
  }

  /**
   * One clustered leading-key interval `(lower, upper]` under a specific key selection (`config`).
   * `lower = None` means unbounded below (a FULL / first backfill). Persisted, alongside the
   * watermark, in the `optimize.cluster.state` table property so it survives snapshot expiration.
   */
  case class ClusterInterval(
      config: String, keys: String, mode: String, lower: Option[String], upper: String)

  /** Stable, compact identity of a key selection: only a keys/mode change produces a new id. */
  def configId(keys: Seq[String], sortMode: String): String = {
    val normalized = keys.map(_.trim).mkString(",") + "|" + sortMode.toLowerCase(Locale.ROOT)
    val crc = new CRC32()
    crc.update(normalized.getBytes(StandardCharsets.UTF_8))
    java.lang.Long.toHexString(crc.getValue)
  }

  /**
   * Parse interval state. Empty / absent input is no state (a fresh table). Non-empty but
   * unparseable input is a corrupted property, not "no state" -- silently treating it as empty
   * would make OPTIMIZE recluster from scratch and mis-report ANALYZE coverage, so it fails loudly
   * naming the property and how to clear it.
   */
  def parseState(json: String): Seq[ClusterInterval] = {
    if (json == null || json.trim.isEmpty) return Seq.empty
    try {
      stateMapper.readValue[Seq[ClusterInterval]](json)
    } catch {
      case NonFatal(e) =>
        throw new IllegalStateException(
          s"Malformed clustering state in table property '$STATE_PROP'; OPTIMIZE cannot tell " +
            s"what is already clustered. Clear the clustering metadata and let the next OPTIMIZE " +
            s"rebuild it: ALTER TABLE <table> UNSET TBLPROPERTIES " +
            s"('$STATE_PROP', '$HWM_PROP', '$CONFIG_ID_PROP'). Value was: $json", e)
    }
  }

  /**
   * Fold a completed run into the interval state. A same-config incremental run extends the current
   * epoch's upper bound (keeping its lower); a config change appends a new epoch, retains the old
   * ones (durable key-selection history); FULL collapses the current config to one unbounded-below
   * interval.
   */
  def advanceState(
      existing: Seq[ClusterInterval],
      cfgId: String,
      keys: Seq[String],
      mode: String,
      lower: Option[String],
      upper: String,
      full: Boolean): Seq[ClusterInterval] = {
    val keysStr = keys.mkString(",")
    val others = existing.filterNot(_.config == cfgId)
    (full, existing.find(_.config == cfgId)) match {
      case (true, _) => others :+ ClusterInterval(cfgId, keysStr, mode, None, upper)
      case (false, Some(cur)) => others :+ cur.copy(keys = keysStr, mode = mode, upper = upper)
      case (false, None) => existing :+ ClusterInterval(cfgId, keysStr, mode, lower, upper)
    }
  }
}
