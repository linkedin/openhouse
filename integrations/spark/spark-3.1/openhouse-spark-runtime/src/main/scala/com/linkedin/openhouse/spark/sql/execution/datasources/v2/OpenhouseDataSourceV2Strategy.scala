package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.{GrantRevokeStatement, SetColumnPolicyTag, SetHistoryPolicy, SetReplicationPolicy, SetRetentionPolicy, SetSharingPolicy, ShowGrantsStatement, UnSetReplicationPolicy}
import org.apache.iceberg.spark.{Spark3Util, SparkCatalog, SparkSessionCatalog}
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{AlterTableSetPropertiesCommand, AlterTableUnsetPropertiesCommand, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.JavaConverters._

/* Strategy to convert a logical plan to physical plans */
case class OpenhouseDataSourceV2Strategy(spark: SparkSession) extends Strategy with PredicateHelper {
  private val ENCRYPTED_PROP_KEY = "encrypted"

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case SetRetentionPolicy(CatalogAndIdentifierExtractor(catalog, ident), granularity, count, colName, colPattern) =>
      SetRetentionPolicyExec(catalog, ident, granularity, count, colName, colPattern) :: Nil
    case SetReplicationPolicy(CatalogAndIdentifierExtractor(catalog, ident), replicationPolicies) =>
      SetReplicationPolicyExec(catalog, ident, replicationPolicies) :: Nil
    case UnSetReplicationPolicy(CatalogAndIdentifierExtractor(catalog, ident), replicationPolicies) =>
      UnSetReplicationPolicyExec(catalog, ident, replicationPolicies) :: Nil
    case SetHistoryPolicy(CatalogAndIdentifierExtractor(catalog, ident), granularity, maxAge, versions) =>
      SetHistoryPolicyExec(catalog, ident, granularity, maxAge, versions) :: Nil
    case SetSharingPolicy(CatalogAndIdentifierExtractor(catalog, ident), sharing) =>
      SetSharingPolicyExec(catalog, ident, sharing) :: Nil
    case SetColumnPolicyTag(CatalogAndIdentifierExtractor(catalog, ident), policyTag, cols) =>
      SetColumnPolicyTagExec(catalog, ident, policyTag, cols) :: Nil

    case GrantRevokeStatement(isGrant, resourceType, CatalogAndIdentifierExtractor(catalog, ident), privilege, principal) =>
      GrantRevokeStatementExec(isGrant, resourceType, catalog, ident, privilege, principal) :: Nil

    case r @ ShowGrantsStatement(resourceType, CatalogAndIdentifierExtractor(catalog, ident)) =>
      ShowGrantsStatementExec(r.output, resourceType, catalog, ident) :: Nil

    case AlterTableSetPropertiesCommand(CatalogAndIdentifierExtractor(catalog, ident), props, isView) if !isView =>
      catalog.loadTable(ident) match {
        case iceberg: SparkTable =>
          val currentProps = iceberg.table().properties()
          val isCurrentlyEncrypted = currentProps.getOrDefault(ENCRYPTED_PROP_KEY, "false").toBoolean
          if (isCurrentlyEncrypted && props.contains(ENCRYPTED_PROP_KEY)) {
            if (!props(ENCRYPTED_PROP_KEY).equalsIgnoreCase("true")) {
              throw new UnsupportedOperationException(s"Cannot modify or unset the '$ENCRYPTED_PROP_KEY' property for table $ident once set to true.")
            }
          }
          Nil
        case other =>
          Nil
      }

    case AlterTableUnsetPropertiesCommand(CatalogAndIdentifierExtractor(catalog, ident), propKeys, ifExists, isView) if !isView =>
      catalog.loadTable(ident) match {
        case iceberg: SparkTable =>
          val currentProps = iceberg.table().properties()
          val isCurrentlyEncrypted = currentProps.getOrDefault(ENCRYPTED_PROP_KEY, "false").toBoolean
          val unsettingEncryption = propKeys.exists(_.equalsIgnoreCase(ENCRYPTED_PROP_KEY))

          if (isCurrentlyEncrypted && unsettingEncryption) {
            throw new UnsupportedOperationException(s"Cannot modify or unset the '$ENCRYPTED_PROP_KEY' property for table $ident once set to true.")
          } else {
            Nil
          }
        case other =>
          Nil
      }

    case _ => Nil
  }

  private object CatalogAndIdentifierExtractor {
    def unapply(identifier: Seq[String]): Option[(TableCatalog, Identifier)] = {
      val catalogAndIdentifier = Spark3Util.catalogAndIdentifier(spark, identifier.asJava)
      catalogAndIdentifier.catalog match {
        case icebergCatalog: SparkCatalog =>
          Some((icebergCatalog, catalogAndIdentifier.identifier))
        case icebergCatalog: SparkSessionCatalog[_] =>
          Some((icebergCatalog, catalogAndIdentifier.identifier))
        case _ =>
          None
      }
    }
  }
}
