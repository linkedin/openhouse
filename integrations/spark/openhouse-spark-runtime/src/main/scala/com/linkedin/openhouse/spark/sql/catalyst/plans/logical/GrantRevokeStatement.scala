package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import com.linkedin.openhouse.spark.sql.catalyst.enums.GrantableResourceTypes.GrantableResourceType
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}

case class GrantRevokeStatement(isGrant: Boolean, resourceType: GrantableResourceType, resourceName: Seq[String], privilege: String, principal: String) extends Command {
  override def simpleString(maxFields: Int): String = {
    s"GrantRevokeStatement: isGrant ${isGrant}, ${resourceType} ${resourceName} ${privilege} ${principal}"
  }

  override def children: Seq[LogicalPlan] = Seq.empty

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    legacyWithNewChildren(newChildren)
  }
}
