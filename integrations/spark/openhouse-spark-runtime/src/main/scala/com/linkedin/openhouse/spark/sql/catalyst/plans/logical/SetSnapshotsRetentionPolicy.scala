package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.Command

case class SetSnapshotsRetentionPolicy (tableName: Seq[String], granularity: String, timeCount: Int, count: Int) extends Command {
  override def simpleString(maxFields: Int): String = {
    s"SetSnapshotsRetentionPolicy: ${tableName} ${timeCount} ${granularity} ${count}"
  }
}
