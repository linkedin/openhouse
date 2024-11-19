package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.linkedin.openhouse.javaclient.api.SupportsGrantRevoke
import com.linkedin.openhouse.spark.sql.catalyst.enums.GrantableResourceTypes
import com.linkedin.openhouse.spark.sql.catalyst.enums.GrantableResourceTypes.GrantableResourceType
import com.linkedin.openhouse.spark.sql.execution.datasources.v2.mapper.IcebergCatalogMapper
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._


case class ShowGrantsStatementExec(
  output: Seq[Attribute],
  resourceType: GrantableResourceType,
  catalog: TableCatalog,
  ident: Identifier) extends LeafV2CommandExec with LeafExecNode {

  override protected def run(): Seq[InternalRow] = {
    /** Extract {@link OpenHouseCatalog}  from {@link TableCatalog} */
    IcebergCatalogMapper.toIcebergCatalog(catalog) match {
      /** Call {@link SupportsGrantRevoke#updateTableAclPolicies} or {@link SupportsGrantRevoke#updateDatabaseAclPolicies} */
      case grantRevokableCatalog: SupportsGrantRevoke =>
        (resourceType match {
          case GrantableResourceTypes.TABLE =>
            grantRevokableCatalog.getTableAclPolicies(Spark3Util.identifierToTableIdentifier(ident))
          case GrantableResourceTypes.DATABASE =>
            grantRevokableCatalog.getDatabaseAclPolicies(toNamespace(ident))
        }).asScala.map { aclPolicy =>
          val row: Array[Any] = Array(UTF8String.fromString(aclPolicy.getPrivilege), UTF8String.fromString(aclPolicy.getPrincipal))
          new GenericInternalRow(row)
        }
      case _ =>
        throw new UnsupportedOperationException(s"Catalog '${catalog.name()}' does not support Grant Revoke Statements")
    }
  }

  override def simpleString(maxFields: Int): String = {
    s"ShowGrantsStatementExec: ${catalog.name()} $ident"
  }

  private def toNamespace(ident: Identifier): Namespace = {
    val dbArray: Array[String] = ident.namespace() :+ ident.name()
    Namespace.of(dbArray:_*)
  }
}
