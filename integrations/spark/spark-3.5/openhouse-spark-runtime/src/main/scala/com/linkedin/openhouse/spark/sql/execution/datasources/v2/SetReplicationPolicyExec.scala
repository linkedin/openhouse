package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

case class SetReplicationPolicyExec(catalog: TableCatalog, ident: Identifier, replicationPolicies: Seq[(String, Option[String])]) extends LeafV2CommandExec{
  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable if iceberg.table().properties().containsKey("openhouse.tableId") =>
        val key = "updated.openhouse.policy"
        val value = s"""{"replication":{"config":[${replicationPolicies.map(replication => s"""{"destination":"${replication._1}","interval":"${replication._2.getOrElse("")}"}""").mkString(",")}]}}"""
        iceberg.table().updateProperties()
          .set(key, value)
          .commit()

      case table =>
        throw new UnsupportedOperationException(s"Cannot set replication policy for non-Openhouse table: $table")
    }
    Nil
  }

  override def output: Seq[Attribute] = Nil
}
