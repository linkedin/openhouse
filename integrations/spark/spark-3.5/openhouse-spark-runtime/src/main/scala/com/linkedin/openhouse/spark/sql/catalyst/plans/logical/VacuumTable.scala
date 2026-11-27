package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.LeafCommand

case class VacuumTable(tableName: Seq[String], removeOrphanFiles: Boolean, retainHours: Option[Int]) extends LeafCommand {
  override def simpleString(maxFields: Int): String = {
    s"VacuumTable: ${tableName} removeOrphanFiles=${removeOrphanFiles} retainHours=${retainHours.getOrElse("default")}"
  }
}
