package com.linkedin.openhouse.spark.sql.catalyst.enums

private[sql] object LogicalOperators extends Enumeration {
  type LogicalOperatorsType = Value
  val AND, OR = Value
}
