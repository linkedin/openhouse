package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.Command

case class SetHistoryPolicy (tableName: Seq[String], granularity: Option[String], maxAge: Int, minVersions: Int) extends Command {
  override def simpleString(maxFields: Int): String = {
    s"SetHistoryPolicy: ${tableName} ${if (maxAge > 0) "MAX_AGE=" + maxAge else ""}${granularity.getOrElse("")} ${if (minVersions > 0) "MIN_VERSIONS=" + minVersions else ""}"
  }
}
