package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.Command

sealed trait TableLockOperation

object TableLockOperation {
  case object Lock extends TableLockOperation
  case object Unlock extends TableLockOperation
}

case class TableLockStatement(
  operation: TableLockOperation,
  tableName: Seq[String],
  reason: Option[String],
  message: Option[String]) extends Command {

  override def simpleString(maxFields: Int): String = {
    s"TableLockStatement: $operation $tableName $reason messagePresent=${message.isDefined}"
  }
}
