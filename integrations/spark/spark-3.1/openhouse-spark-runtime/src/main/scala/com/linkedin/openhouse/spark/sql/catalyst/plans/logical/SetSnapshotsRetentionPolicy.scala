package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.Command

case class SetSnapshotsRetentionPolicy (tableName: Seq[String], granularity: Option[String], timeCount: Int, count: Int) extends Command {
  override def simpleString(maxFields: Int): String = {
    s"SetSnapshotsRetentionPolicy: ${tableName} ${if (timeCount > 0) timeCount else ""} ${granularity.getOrElse("")} count ${if (count > 0) count else ""}"
  }
}
