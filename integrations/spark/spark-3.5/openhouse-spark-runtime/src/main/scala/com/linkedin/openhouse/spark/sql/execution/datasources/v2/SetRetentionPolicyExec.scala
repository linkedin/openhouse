package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

case class SetRetentionPolicyExec(
  catalog: TableCatalog,
  ident: Identifier,
  granularity: String,
  count: Int,
  colName: Option[String],
  colPattern: Option[String]
                                 ) extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable if iceberg.table().properties().containsKey("openhouse.tableId") =>
        val key = "updated.openhouse.policy"
        val value = {
          (colName, colPattern) match {
            case (None, None) => s"""{"retention":{"count":${count},"granularity":"${granularity}"}}"""
            case (Some(nameVal), Some(patternVal)) => {
              val columnPattern = s"""{"columnName":"${nameVal}","pattern": "${patternVal}"}"""
              s"""{"retention":{"count":${count},"granularity":"${granularity}", "columnPattern":${columnPattern}}}"""
            }
            case (Some(nameVal), None) => {
              val columnPattern = s"""{"columnName":"${nameVal}","pattern": ""}"""
              s"""{"retention":{"count":${count},"granularity":"${granularity}", "columnPattern":${columnPattern}}}"""
            }
          }
        }

        iceberg.table().updateProperties()
          .set(key, value)
          .commit()

      case table =>
        throw new UnsupportedOperationException(s"Cannot set retention policy for non-Openhouse table: $table")
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"SetRetentionPolicyExec: ${catalog} ${ident} ${count} ${granularity} ${colName.getOrElse("")} ${colPattern.getOrElse("")}"
  }
}
