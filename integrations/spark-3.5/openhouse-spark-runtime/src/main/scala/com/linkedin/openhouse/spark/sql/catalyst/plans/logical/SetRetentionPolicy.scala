package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.LeafCommand

case class SetRetentionPolicy(tableName: Seq[String], granularity: String, count: Int, colName: Option[String], colPattern: Option[String]) extends LeafCommand {
  override def simpleString(maxFields: Int): String = {
    s"SetRetentionPolicy: ${tableName} ${count} ${granularity} ${colName.getOrElse("")} ${colPattern.getOrElse("")}"
  }
}
