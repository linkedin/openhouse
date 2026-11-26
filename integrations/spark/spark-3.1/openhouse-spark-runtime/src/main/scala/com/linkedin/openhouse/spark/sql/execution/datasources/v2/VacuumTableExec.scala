package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.linkedin.openhouse.spark.maintenance.TableMaintenance
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

case class VacuumTableExec(
  output: Seq[Attribute],
  catalog: TableCatalog,
  ident: Identifier,
  dryRun: Boolean) extends V2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val result = catalog.loadTable(ident) match {
      case iceberg: SparkTable if iceberg.table().properties().containsKey("openhouse.tableId") =>
        new TableMaintenance(SparkSession.active).vacuum(iceberg.table(), dryRun)
      case table =>
        throw new UnsupportedOperationException(s"Cannot vacuum non-Openhouse table: $table")
    }
    if (!dryRun) {
      catalog.invalidateTable(ident)
    }
    result.asScala.toSeq.map { case (metric, value) =>
      new GenericInternalRow(Array[Any](UTF8String.fromString(metric), value.longValue()))
    }
  }

  override def simpleString(maxFields: Int): String = {
    s"VacuumTableExec: ${catalog.name()} $ident${if (dryRun) " DRY RUN" else ""}"
  }
}
