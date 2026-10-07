package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.{GrantRevokeStatement, SetColumnPolicyTag, SetHistoryPolicy, SetReplicationPolicy, SetRetentionPolicy, SetSharingPolicy, ShowGrantsStatement, UnSetReplicationPolicy}
import org.apache.iceberg.spark.{Spark3Util, SparkCatalog, SparkSessionCatalog}
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{DropTable, LogicalPlan, RenameTable}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.JavaConverters._

/* Strategy to convert a logical plan to physical plans */
case class OpenhouseDataSourceV2Strategy(spark: SparkSession) extends Strategy with PredicateHelper {
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

    case DropTable(identifier: ResolvedIdentifier, ifExists, purge) =>
      identifier.catalog match {
        case catalog: TableCatalog
            if ReplicationDdlForwarder.isOpenHouseCatalog(catalog) &&
              ReplicationDdlForwarder.cascadeEnabled(
                spark.conf.get(ReplicationDdlForwarder.CascadeConfig, "true")) =>
          val table =
            try {
              catalog.loadTable(identifier.identifier)
            } catch {
              case _: org.apache.iceberg.exceptions.NoSuchTableException if ifExists => return Nil
              case _: org.apache.spark.sql.catalyst.analysis.NoSuchTableException if ifExists =>
                return Nil
            }
          val destinations =
            ReplicationDdlForwarder.replicationDestinations(table.properties())
          if (destinations.isEmpty) {
            Nil
          } else {
            ReplicatedDropTableExec(
              spark,
              catalog,
              identifier.identifier,
              table,
              destinations,
              ifExists,
              purge) :: Nil
          }
        case _ => Nil
      }

    case RenameTable(table: ResolvedTable, newIdentifier, isView)
        if !isView &&
          ReplicationDdlForwarder.isOpenHouseCatalog(table.catalog) &&
          ReplicationDdlForwarder.cascadeEnabled(
            spark.conf.get(ReplicationDdlForwarder.CascadeConfig, "true")) =>
      val catalog = table.catalog
      val from = table.identifier
      val to = Identifier.of(newIdentifier.dropRight(1).toArray, newIdentifier.last)
      val destinations =
        ReplicationDdlForwarder.replicationDestinations(table.table.properties())
      if (destinations.isEmpty) {
        Nil
      } else {
        ReplicatedRenameTableExec(spark, catalog, from, to, table.table, destinations) :: Nil
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
