package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.linkedin.openhouse.javaclient.api.SupportsUnlock
import com.linkedin.openhouse.spark.sql.execution.datasources.v2.mapper.IcebergCatalogMapper
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

case class UnlockTableExec(
  catalog: TableCatalog,
  ident: Identifier,
  reason: Option[String]) extends V2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    // Calls the catalog directly because loading a locked table can fail.
    IcebergCatalogMapper.toIcebergCatalog(catalog) match {
      case unlockableCatalog: SupportsUnlock =>
        unlockableCatalog.unlockTable(Spark3Util.identifierToTableIdentifier(ident), reason.orNull)
        // Drop the cached table so this session does not keep showing the removed lock.
        catalog.invalidateTable(ident)
      case _ =>
        throw new UnsupportedOperationException(s"Catalog '${catalog.name()}' does not support UNLOCK")
    }
    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"UnlockTableExec: ${catalog.name()} $ident ${reason.getOrElse("")}"
  }
}
