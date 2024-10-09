package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.Command

case class SetReplicationPolicy(tableName: Seq[String], replicationPolicies: Seq[String]) extends Command {
  override def simpleString(maxFields: Int): String = {
    s"SetReplicationPolicy: ${tableName} ${replicationPolicies}}"
  }
}
