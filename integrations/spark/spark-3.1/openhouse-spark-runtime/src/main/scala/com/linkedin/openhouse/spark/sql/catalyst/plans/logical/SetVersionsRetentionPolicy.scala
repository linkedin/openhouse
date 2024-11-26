package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.Command
import com.linkedin.openhouse.spark.sql.catalyst.enums.LogicalOperators.LogicalOperatorsType

case class SetVersionsRetentionPolicy(tableName: Seq[String], logicalOperator: Option[LogicalOperatorsType], granularity: Option[String], timeCount: Int, count: Int) extends Command {
  override def simpleString(maxFields: Int): String = {
    s"SetVersionsRetentionPolicy: ${tableName} ${if (timeCount > 0) timeCount else ""} ${granularity.getOrElse("")} ${logicalOperator.getOrElse("")} count ${if (count > 0) count else ""}"
  }
}
