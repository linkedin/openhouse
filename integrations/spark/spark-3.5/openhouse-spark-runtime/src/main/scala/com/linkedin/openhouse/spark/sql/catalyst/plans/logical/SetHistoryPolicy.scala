package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.LeafCommand

case class SetHistoryPolicy(tableName: Seq[String], granularity: Option[String], maxAge: Int, versions: Int) extends LeafCommand {
  override def simpleString(maxFields: Int): String = {
    s"SetHistoryPolicy: ${tableName} ${if (maxAge > 0) "MAX_AGE=" + maxAge else ""}${granularity.getOrElse("")} ${if (versions > 0) "VERSIONS=" + versions else ""}"
  }
}
