package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.linkedin.openhouse.gen.tables.client.model.CreateUpdateLockRequestBody.ReasonEnum
import com.linkedin.openhouse.javaclient.api.{SupportsUnlock, UnlockTableTarget}
import com.linkedin.openhouse.spark.sql.execution.datasources.v2.mapper.IcebergCatalogMapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

case class UnlockTableExec(
  catalog: TableCatalog,
  target: UnlockTableTarget,
  reason: ReasonEnum) extends V2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    // Skips loading, which fails on a SYSTEM_ONLY lock.
    IcebergCatalogMapper.toIcebergCatalog(catalog) match {
      case unlockableCatalog: SupportsUnlock =>
        unlockableCatalog.unlockTable(target, reason)
        // Clear the session's cached lock.
        catalog.invalidateTable(Identifier.of(Array(target.getDatabaseId), target.getTableId))
      case _ =>
        throw new UnsupportedOperationException(s"Catalog '${catalog.name()}' does not support UNLOCK")
    }
    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"UnlockTableExec: ${catalog.name()} $target $reason"
  }
}
