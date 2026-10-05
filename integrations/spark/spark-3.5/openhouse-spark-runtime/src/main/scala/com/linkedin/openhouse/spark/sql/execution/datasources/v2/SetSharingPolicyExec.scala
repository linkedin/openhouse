package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.linkedin.openhouse.spark.sql.execution.datasources.v2.mapper.IcebergCatalogMapper
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

case class SetSharingPolicyExec(
  catalog: TableCatalog,
  ident: Identifier,
  sharing: String) extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable
          if IcebergCatalogMapper.toIcebergCatalog(catalog).isInstanceOf[
            com.linkedin.openhouse.spark.OpenHouseCatalog] =>
        val key = "updated.openhouse.policy"
        val value = s"""{"sharingEnabled": ${sharing}}"""

        iceberg.table().updateProperties()
          .set(key, value)
          .commit()

      case table =>
        throw new UnsupportedOperationException(s"Cannot set sharing policy for non-Openhouse table: $table")
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"SetSharingPolicyExec: ${catalog} ${ident} ${sharing}"
  }
}
