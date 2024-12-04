package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

case class SetSnapshotsRetentionPolicyExec(
                                            catalog: TableCatalog,
                                            ident: Identifier,
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
          (timeCount, count) match {
            case ttlOnly if count == -1 => s"""{"snapshotsRetention":{"timeCount":${timeCount},"granularity":"${granularity.get}"}}"""
            case countOnly if timeCount == -1 => s"""{"snapshotsRetention":{"count":${count}}}"""
            case _ => s"""{"snapshotsRetention":{"timeCount":${timeCount},"granularity":"${granularity.get}","count":${count}}}"""
          }
        }

        iceberg.table().updateProperties()
          .set(key, value)
          .commit()

      case table =>
        throw new UnsupportedOperationException(s"Cannot set snapshot retention policy for non-Openhouse table: $table")
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"SetSnapshotsRetentionPolicyExec: ${catalog} ${ident} ${if (timeCount > 0) timeCount else ""} ${granularity.getOrElse("")} count ${if (count > 0) count else ""}"
  }
}
