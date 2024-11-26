package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.linkedin.openhouse.spark.sql.catalyst.enums.LogicalOperators
import com.linkedin.openhouse.spark.sql.catalyst.enums.LogicalOperators.LogicalOperatorsType
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

case class SetVersionsRetentionPolicyExec(
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
          (timeCount, count, logicalOperator) match {
            case ttlOnly if count == -1 => s"""{"versionsRetention":{"timeCount":${timeCount},"granularity":"${granularity.get}"}}"""
            case countOnly if timeCount == -1 => s"""{"versionsRetention":{"count":${count}}}"""
            case _ => s"""{"versionsRetention":{"timeCount":${timeCount},"granularity":"${granularity.get}","count":${count},"logicalOperator":${logicalOperator.get.toString}}}"""}
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
    s"SetVersionsRetentionPolicyExec: ${catalog} ${ident} ${if (timeCount > 0) timeCount else ""} ${granularity.getOrElse("")} ${logicalOperator.getOrElse("")} count ${if (count > 0) count else ""}"
  }
}
