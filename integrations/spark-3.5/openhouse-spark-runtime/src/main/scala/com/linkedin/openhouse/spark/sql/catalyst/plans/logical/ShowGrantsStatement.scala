package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import com.linkedin.openhouse.spark.sql.catalyst.enums.GrantableResourceTypes.GrantableResourceType
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LeafCommand
import org.apache.spark.sql.types.StringType


case class ShowGrantsStatement(resourceType: GrantableResourceType, resourceName: Seq[String]) extends LeafCommand {

  override lazy val output: Seq[Attribute] = Seq(
    AttributeReference("privilege", StringType, nullable = false)(),
    AttributeReference("principal", StringType, nullable = false)()
  )
  override def simpleString(maxFields: Int): String = {
    s"ShowGrantsStatement: ${resourceType} ${resourceName}"
  }
}
