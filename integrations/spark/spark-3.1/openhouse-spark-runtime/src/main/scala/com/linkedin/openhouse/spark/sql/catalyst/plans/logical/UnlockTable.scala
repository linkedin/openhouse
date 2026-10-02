package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.Command

case class UnlockTable(tableName: Seq[String], reason: Option[String]) extends Command {
  override def simpleString(maxFields: Int): String = {
    s"UnlockTable: ${tableName} ${reason.getOrElse("")}"
  }
}
