package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.plans.logical.LeafCommand

/**
 * Logical plan for the RENAME TABLE statement.
 *
 * @param fromTable The source table identifier (catalog.database.table)
 * @param toTable The destination table identifier (catalog.database.table)
 */
case class RenameTable(
  fromTable: Seq[String],
  toTable: Seq[String]
) extends LeafCommand {
  override def simpleString(maxFields: Int): String = {
    s"RenameTable: ${fromTable} ${toTable}"
  }
}
