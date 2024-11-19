package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.LeafCommand

case class SetReplicationPolicy(tableName: Seq[String], replicationPolicies: Seq[(String, Option[String])]) extends LeafCommand {
  override def simpleString(maxFields: Int): String = {
    s"SetReplicationPolicy: ${tableName} ${replicationPolicies}"
  }
}
