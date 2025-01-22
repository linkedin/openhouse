package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.Command

case class SetWorldReadablePolicy(tableName: Seq[String], isWorldReadable: String) extends Command {
  override def simpleString(maxFields: Int): String = {
    s"SetWorldReadablePolicy: ${tableName} ${isWorldReadable}"
  }
}
