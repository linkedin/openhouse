package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.{GrantRevokeStatement, SetRetentionPolicy, SetSharingPolicy, SetColumnPolicyTag, ShowGrantsStatement}
import org.apache.iceberg.spark.{Spark3Util, SparkCatalog, SparkSessionCatalog}
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.JavaConverters._

/* Strategy to convert a logical plan to physical plans */
case class OpenhouseDataSourceV2Strategy(spark: SparkSession) extends Strategy with PredicateHelper {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case SetRetentionPolicy(CatalogAndIdentifierExtractor(catalog, ident), granularity, count, colName, colPattern) =>
      SetRetentionPolicyExec(catalog, ident, granularity, count, colName, colPattern) :: Nil
    case SetSharingPolicy(CatalogAndIdentifierExtractor(catalog, ident), sharing) =>
      SetSharingPolicyExec(catalog, ident, sharing) :: Nil
    case SetColumnPolicyTag(CatalogAndIdentifierExtractor(catalog, ident), policyTag, cols) =>
      SetColumnPolicyTagExec(catalog, ident, policyTag, cols) :: Nil

    case GrantRevokeStatement(isGrant, resourceType, CatalogAndIdentifierExtractor(catalog, ident), privilege, principal) =>
      GrantRevokeStatementExec(isGrant, resourceType, catalog, ident, privilege, principal) :: Nil

    case r @ ShowGrantsStatement(resourceType, CatalogAndIdentifierExtractor(catalog, ident)) =>
      ShowGrantsStatementExec(r.output, resourceType, catalog, ident) :: Nil

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
