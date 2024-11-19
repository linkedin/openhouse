package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import com.linkedin.openhouse.spark.sql.catalyst.enums.GrantableResourceTypes.GrantableResourceType
import org.apache.spark.sql.catalyst.plans.logical.LeafCommand

case class GrantRevokeStatement(isGrant: Boolean, resourceType: GrantableResourceType, resourceName: Seq[String], privilege: String, principal: String) extends LeafCommand {
  override def simpleString(maxFields: Int): String = {
    s"GrantRevokeStatement: isGrant ${isGrant}, ${resourceType} ${resourceName} ${privilege} ${principal}"
  }
}
