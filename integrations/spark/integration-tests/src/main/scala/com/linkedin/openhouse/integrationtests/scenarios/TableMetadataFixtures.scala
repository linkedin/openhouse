package com.linkedin.openhouse.integrationtests

import org.apache.spark.sql.SparkSession

/** Metadata helpers shared by table-state and changelog fixtures. */
trait TableMetadataFixtures {
  this: TableTestFixtures =>

  /** Snapshot identifiers in ancestry order, with the root snapshot first. */
  protected def snapshotIds(spark: SparkSession, table: String): Seq[Long] = {
    val rows = spark.sql(s"SELECT snapshot_id, parent_id FROM $table.snapshots").collect().toSeq
    val snapshotIdSet = rows.map(_.getLong(0)).toSet
    val childByParent = rows.collect {
      case row if !row.isNullAt(1) => row.getLong(1) -> row.getLong(0)
    }.toMap
    val root = rows.collectFirst {
      case row if row.isNullAt(1) || !snapshotIdSet.contains(row.getLong(1)) => row.getLong(0)
    }.get

    Iterator
      .iterate(Option(root))(parent => parent.flatMap(childByParent.get))
      .takeWhile(_.isDefined)
      .flatten
      .toList
  }

  protected def catalogRelativeTableName(table: String): String =
    table.stripPrefix("openhouse.")
}
