package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.LeafCommand

case class SetColumnPolicyTag(tableName: Seq[String], colName: String, policyTags: Seq[String]) extends LeafCommand {
  override def simpleString(maxFields: Int): String = {
    s"SetColumnPolicyTag: ${tableName} ${colName} ${policyTags}"
  }
}
