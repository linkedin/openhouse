package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.LeafCommand

case class SetSharingPolicy(tableName: Seq[String], sharing: String) extends LeafCommand {
  override def simpleString(maxFields: Int): String = {
    s"SetSharingPolicy: ${tableName} ${sharing}"
  }
}
