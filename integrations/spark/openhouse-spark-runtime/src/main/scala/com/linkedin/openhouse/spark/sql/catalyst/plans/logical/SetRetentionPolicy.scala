package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.Command

case class SetRetentionPolicy(tableName: Seq[String], granularity: String, count: Int, colName: Option[String], colPattern: Option[String]) extends Command {
  override def simpleString(maxFields: Int): String = {
    s"SetRetentionPolicy: ${tableName} ${count} ${granularity} ${colName.getOrElse("")} ${colPattern.getOrElse("")}"
  }
}
