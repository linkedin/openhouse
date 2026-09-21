package com.linkedin.openhouse.integrationtests.delta

import org.apache.spark.sql.SparkSession

/**
 * Partition, lineage, metadata, and rename fixtures used by replace-table scenarios.
 */
trait RtasTableFixtures extends DmlTableFixtures {

  /** One partitioning choice and the CREATE clause that applies it. */
  final case class Partitioning(label: String, clause: String)

  protected val unpartitioned = Partitioning("unpartitioned", "")
  protected val partitionedByDate =
    Partitioning("partitioned", s"PARTITIONED BY (${Core.date0.columnName})")
  protected val partitionings: List[Partitioning] = List(unpartitioned, partitionedByDate)

  /** One core row in the seed shape, keyed by `long` and tagged in the string column. */
  protected def coreRow(long: Long, tag: String): String = {
    val booleanLiteral = if (long % 2 == 0) "true" else "false"
    s"(CAST($long AS BIGINT), ${long.toInt}, '$tag', ${long}.5, $booleanLiteral, " +
      s"'${Core.dateLiteral(long.toInt)}')"
  }

  protected def tableProperties(spark: SparkSession, table: String): Map[String, String] =
    spark.sql(s"SHOW TBLPROPERTIES $table").collect().toSeq
      .map(row => row.getString(0) -> row.getString(1))
      .toMap

  protected def queryCount(spark: SparkSession, sql: String): String =
    spark.sql(sql).collect()(0).getLong(0).toString

  /** Tracks an accepted rename and drops the table under its live name after the operation completes. */
  protected def withTrackedRename(runStatement: String => Unit, originalTable: String)(
      use: (String => Unit) => Unit): Unit = {
    var liveTable = originalTable
    OwnedTableLifecycle.withCleanup(
      if (liveTable != originalTable) runStatement(s"DROP TABLE IF EXISTS $liveTable")) {
      use { newTable =>
        runStatement(s"ALTER TABLE $liveTable RENAME TO $newTable")
        liveTable = newTable
      }
    }
  }
}
