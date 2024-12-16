package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

case class SetHistoryPolicyExec(
                                            catalog: TableCatalog,
                                            ident: Identifier,
                                            granularity: Option[String],
                                            maxAge: Int,
                                            versions: Int
) extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable if iceberg.table().properties().containsKey("openhouse.tableId") =>
        val key = "updated.openhouse.policy"
        val value = {
          (maxAge, versions) match {
            case maxAgeOnly if versions == -1 => s"""{"history":{"maxAge":${maxAge},"granularity":"${granularity.get}"}}"""
            case versionsOnly if maxAge == -1 => s"""{"history":{"versions":${versions}}}"""
            case _ => s"""{"history":{"maxAge":${maxAge},"granularity":"${granularity.get}","versions":${versions}}}"""
          }
        }

        iceberg.table().updateProperties()
          .set(key, value)
          .commit()

      case table =>
        throw new UnsupportedOperationException(s"Cannot set history policy for non-Openhouse table: $table")
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"SetHistoryPolicyExec: ${catalog} ${ident} MAX_AGE=${if (maxAge > 0) maxAge else ""}${granularity.getOrElse("")} VERSIONS=${if (versions > 0) versions else ""}"
  }
}
