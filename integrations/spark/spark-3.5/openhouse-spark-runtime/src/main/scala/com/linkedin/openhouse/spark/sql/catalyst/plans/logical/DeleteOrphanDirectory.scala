package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LeafCommand
import org.apache.spark.sql.types.StringType

case class DeleteOrphanDirectory(path: String, dryRun: Boolean) extends LeafCommand {

  override lazy val output: Seq[Attribute] = Seq(
    AttributeReference("path", StringType, nullable = false)(),
    AttributeReference("owner", StringType, nullable = false)(),
    AttributeReference("action", StringType, nullable = false)()
  )

  override def simpleString(maxFields: Int): String = {
    s"DeleteOrphanDirectory: $path${if (dryRun) " DRY RUN" else ""}"
  }
}
