package com.linkedin.openhouse.spark.sql.catalyst.analysis

import com.linkedin.openhouse.javaclient.api.SupportsUnlock
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.ResolvedUnlockTable
import com.linkedin.openhouse.spark.sql.execution.datasources.v2.mapper.IcebergCatalogMapper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf

object CheckUnlockTable extends (LogicalPlan => Unit) {
  val UNLOCK_ENABLED = SQLConf.buildConf("spark.sql.openhouse.unlock.enabled")
    .doc("Enable ALTER TABLE UNLOCK. The catalog's authenticated caller must also have lock-administration permission.")
    .booleanConf
    .createWithDefault(false)

  override def apply(plan: LogicalPlan): Unit = plan.foreach {
    case ResolvedUnlockTable(catalog, target, _) =>
      IcebergCatalogMapper.toIcebergCatalog(catalog) match {
        case unlockable: SupportsUnlock =>
          // Use the catalog's credentials, not a client-supplied owner name or table load.
          unlockable.checkUnlockAuthorization(target)
        case _ =>
          throw new UnsupportedOperationException(s"Catalog '${catalog.name()}' does not support UNLOCK")
      }
    case _ =>
  }
}
