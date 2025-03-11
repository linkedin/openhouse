package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.Command

case class UnSetReplicationPolicy(tableName: Seq[String], replicationPolicies: String) extends Command {
  override def simpleString(maxFields: Int): String = {
    s"UnSetReplicationPolicy: ${tableName}"
  }
}
