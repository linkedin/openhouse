package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, LeafV2CommandExec}

case class ReplicatedRenameTableExec(
    spark: SparkSession,
    catalog: TableCatalog,
    from: Identifier,
    to: Identifier,
    table: Table,
    replicationDestinations: Seq[String])
    extends LeafV2CommandExec {
  override protected def run(): Seq[InternalRow] = {
    val destinations =
      replicationDestinations.map(
        ReplicationDdlForwarder.destinationCatalog(spark, catalog, _))
    val destinationTables =
      destinations.map(destination => (destination, destination.loadTable(from)))

    uncache(catalog, from, table)
    destinationTables.foreach {
      case (destination, destinationTable) => uncache(destination, from, destinationTable)
    }
    catalog.renameTable(from, to)
    destinationTables.foreach {
      case (destination, _) =>
        destination.renameTable(
          from,
          ReplicationDdlForwarder.identifierForDestination(catalog.name(), to))
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
