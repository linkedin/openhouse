package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, LeafV2CommandExec}

case class ReplicatedDropTableExec(
    spark: SparkSession,
    catalog: TableCatalog,
    ident: Identifier,
    table: Table,
    replicationDestinations: Seq[String],
    ifExists: Boolean,
    purge: Boolean)
    extends LeafV2CommandExec {
  override protected def run(): Seq[InternalRow] = {
    val destinations =
      replicationDestinations.map(
        ReplicationDdlForwarder.destinationCatalog(spark, catalog, _))
    val destinationTables =
      destinations.flatMap { destination =>
        try {
          Some((destination, destination.loadTable(ident)))
        } catch {
          case _: org.apache.iceberg.exceptions.NoSuchTableException if ifExists => None
          case _: org.apache.spark.sql.catalyst.analysis.NoSuchTableException if ifExists => None
        }
      }

    uncache(catalog, ident, table)
    val sourceDropped = if (purge) catalog.purgeTable(ident) else catalog.dropTable(ident)
    if (!sourceDropped) {
      if (ifExists) return Nil
      throw new IllegalStateException(s"OpenHouse table no longer exists: $ident")
    }
    destinationTables.foreach {
      case (destination, destinationTable) =>
        uncache(destination, ident, destinationTable)
        val dropped =
          if (purge) destination.purgeTable(ident) else destination.dropTable(ident)
        if (!dropped && !ifExists) {
          throw new IllegalStateException(
            s"Replicated OpenHouse table does not exist at destination: $ident")
        }
    }
    Nil
  }

  private def uncache(
      tableCatalog: TableCatalog, identifier: Identifier, loadedTable: Table): Unit = {
    val relation =
      DataSourceV2Relation.create(loadedTable, Some(tableCatalog), Some(identifier))
    spark.sharedState.cacheManager.uncacheQuery(spark, relation, cascade = true)
  }

  override def output: Seq[Attribute] = Nil
}
