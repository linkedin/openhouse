package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import com.linkedin.openhouse.gen.tables.client.model.CreateUpdateLockRequestBody.ReasonEnum
import com.linkedin.openhouse.javaclient.api.UnlockTableTarget
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.connector.catalog.TableCatalog

case class UnlockTable(tableName: Seq[String], reason: ReasonEnum) extends Command {
  override lazy val resolved: Boolean = false

  override def simpleString(maxFields: Int): String = {
    s"UnlockTable: $tableName $reason"
  }
}

case class ResolvedUnlockTable(
  catalog: TableCatalog,
  target: UnlockTableTarget,
  reason: ReasonEnum) extends Command
