package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LeafCommand
import org.apache.spark.sql.types.{LongType, StringType}

case class VacuumTable(tableName: Seq[String], dryRun: Boolean) extends LeafCommand {

  override lazy val output: Seq[Attribute] = Seq(
    AttributeReference("metric", StringType, nullable = false)(),
    AttributeReference("value", LongType, nullable = false)()
  )

  override def simpleString(maxFields: Int): String = {
    s"VacuumTable: $tableName${if (dryRun) " DRY RUN" else ""}"
  }
}
