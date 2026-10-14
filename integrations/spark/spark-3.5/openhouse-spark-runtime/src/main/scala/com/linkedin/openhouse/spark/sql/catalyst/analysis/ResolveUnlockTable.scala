package com.linkedin.openhouse.spark.sql.catalyst.analysis

import com.linkedin.openhouse.javaclient.api.UnlockTableTarget
import com.linkedin.openhouse.javaclient.exception.UnlockValidationException
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.{ResolvedUnlockTable, UnlockTable}
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.TableCatalog

import scala.collection.JavaConverters._

case class ResolveUnlockTable(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case UnlockTable(tableName, reason) =>
      if (!spark.sessionState.conf.getConf(CheckUnlockTable.UNLOCK_ENABLED)) {
        throw new UnsupportedOperationException(
          s"ALTER TABLE UNLOCK is disabled. Set ${CheckUnlockTable.UNLOCK_ENABLED.key}=true to enable it.")
      }
      val resolved = Spark3Util.catalogAndIdentifier(spark, tableName.asJava)
      resolved.catalog match {
        case catalog: TableCatalog =>
          val target = try {
            UnlockTableTarget.of(resolved.identifier.namespace(), resolved.identifier.name())
          } catch {
            case e: UnlockValidationException => throw new UnlockAnalysisException(e)
          }
          ResolvedUnlockTable(catalog, target, reason)
        case _ =>
          throw new UnsupportedOperationException(s"Catalog '${resolved.catalog.name()}' does not support UNLOCK")
      }
  }
}

// Extends AnalysisException to access its protected constructor.
private[analysis] class UnlockAnalysisException(cause: UnlockValidationException)
  extends AnalysisException(cause.getMessage, None, None, cause = Some(cause))
