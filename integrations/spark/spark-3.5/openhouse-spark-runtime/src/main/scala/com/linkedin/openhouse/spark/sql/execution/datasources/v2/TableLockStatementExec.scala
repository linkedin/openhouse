package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.linkedin.openhouse.javaclient.api.SupportsTableLocking
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.TableLockOperation
import com.linkedin.openhouse.spark.sql.execution.datasources.v2.mapper.IcebergCatalogMapper
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

import java.util.Optional

case class TableLockStatementExec(
  operation: TableLockOperation,
  catalog: TableCatalog,
  identifier: Identifier,
  reason: Option[String],
  message: Option[String]) extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    IcebergCatalogMapper.toIcebergCatalog(catalog) match {
      case tableLockingCatalog: SupportsTableLocking =>
        val tableIdentifier = Spark3Util.identifierToTableIdentifier(identifier)
        val optionalReason = reason.map(Optional.of(_)).getOrElse(Optional.empty[String]())
        operation match {
          case TableLockOperation.Lock =>
            tableLockingCatalog.lockTable(
              tableIdentifier,
              optionalReason,
              message.map(Optional.of(_)).getOrElse(Optional.empty[String]()))
          case TableLockOperation.Unlock =>
            tableLockingCatalog.unlockTable(tableIdentifier, optionalReason)
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"Catalog '${catalog.name()}' does not support Table Lock Statements")
    }
    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"TableLockStatementExec: ${catalog.name()} $operation $identifier $reason messagePresent=${message.isDefined}"
  }
}
