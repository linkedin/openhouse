package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.linkedin.openhouse.spark.maintenance.OrphanTableDirectory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

case class DeleteOrphanDirectoryExec(
  output: Seq[Attribute],
  path: String,
  dryRun: Boolean) extends V2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val action = UTF8String.fromString(if (dryRun) "would delete" else "deleted")
    OrphanTableDirectory.deleteContents(SparkSession.active, path, dryRun).asScala.toSeq.map { entry =>
      new GenericInternalRow(Array[Any](
        UTF8String.fromString(entry.path),
        UTF8String.fromString(entry.owner),
        action))
    }
  }

  override def simpleString(maxFields: Int): String = {
    s"DeleteOrphanDirectoryExec: $path${if (dryRun) " DRY RUN" else ""}"
  }
}
