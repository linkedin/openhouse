package com.linkedin.openhouse.spark.sql.catalyst.enums

private[sql] object GrantableResourceTypes extends Enumeration {
  type GrantableResourceType = Value
  val TABLE, DATABASE = Value
}
