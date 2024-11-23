package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.linkedin.openhouse.spark.sql.catalyst.enums.LogicalOperators.LogicalOperatorsType
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

case class SetSnapshotsRetentionPolicyExec(
                                            catalog: TableCatalog,
                                            ident: Identifier,
                                            logicalOperator: Option[LogicalOperatorsType],
                                            granularity: Option[String],
                                            timeCount: Int,
                                            count: Int
) extends V2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable if iceberg.table().properties().containsKey("openhouse.tableId") =>
        val key = "updated.openhouse.policy"
        val value = {
          (count) match {
            case (0) => s"""{"snapshotsRetention":{"timeCount":${timeCount},"granularity":"${granularity}"}}"""
            case (_) =>
              s"""{"snapshotsRetention":{"timeCount":${timeCount}, "granularity":"${granularity}", "count":${count}}}"""
          }
        }

        iceberg.table().updateProperties()
          .set(key, value)
          .commit()

      case table =>
        throw new UnsupportedOperationException(s"Cannot set snapshots retention policy for non-Openhouse table: $table")
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"SetSnapshotsRetentionPolicyExec: ${catalog} ${ident} ${timeCount} ${granularity} ${count}"
  }
}
