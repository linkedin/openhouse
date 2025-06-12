package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

/**
 * Physical plan for renaming a table.
 *
 * @param catalog The table catalog
 * @param fromIdent The source table identifier
 * @param toIdent The destination table identifier
 */
case class RenameTableExec(
    catalog: TableCatalog,
    fromIdent: Identifier,
    toIdent: Identifier) extends V2CommandExec {

  override def output: Seq[Attribute] = Seq.empty

  override def run(): Seq[InternalRow] = {
    catalog.renameTable(fromIdent, toIdent)
    Nil
  }
}
