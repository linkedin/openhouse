package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

case class SetHistoryPolicyExec(
                                            catalog: TableCatalog,
                                            ident: Identifier,
                                            granularity: Option[String],
                                            maxAge: Int,
                                            minVersions: Int
) extends V2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable if iceberg.table().properties().containsKey("openhouse.tableId") =>
        val key = "updated.openhouse.policy"
        val value = {
          (maxAge, minVersions) match {
            case maxAgeOnly if minVersions == -1 => s"""{"history":{"maxAge":${maxAge},"granularity":"${granularity.get}"}}"""
            case minVersionsOnly if maxAge == -1 => s"""{"history":{"minVersions":${minVersions}}}"""
            case _ => s"""{"history":{"maxAge":${maxAge},"granularity":"${granularity.get}","minVersions":${minVersions}}}"""
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
    s"SetHistoryPolicyExec: ${catalog} ${ident} MAX_AGE=${if (maxAge > 0) maxAge else ""}${granularity.getOrElse("")} MIN_VERSIONS=${if (minVersions > 0) minVersions else ""}"
  }
}
