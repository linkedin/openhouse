package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.Command

case class SetHistoryPolicy (tableName: Seq[String], granularity: Option[String], timeCount: Int, count: Int) extends Command {
  override def simpleString(maxFields: Int): String = {
    s"SetHistoryPolicy: ${tableName} ${if (timeCount > 0) "TIME=" + timeCount else ""}${granularity.getOrElse("")} ${if (count > 0) "VERSIONS=" + count else ""}"
  }
}
