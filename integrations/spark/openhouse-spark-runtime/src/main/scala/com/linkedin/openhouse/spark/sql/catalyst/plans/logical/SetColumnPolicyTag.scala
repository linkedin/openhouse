package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}

case class SetColumnPolicyTag(tableName: Seq[String], colName: String, policyTags: Seq[String]) extends Command {
  override def simpleString(maxFields: Int): String = {
    s"SetColumnPolicyTag: ${tableName} ${colName} ${policyTags}"
  }

  override def children: Seq[LogicalPlan] = Seq.empty

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    legacyWithNewChildren(newChildren)
  }
}
