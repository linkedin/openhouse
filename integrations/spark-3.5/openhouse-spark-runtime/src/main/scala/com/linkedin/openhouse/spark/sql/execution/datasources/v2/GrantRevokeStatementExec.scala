package com.linkedin.openhouse.spark.sql.execution.datasources.v2


import com.linkedin.openhouse.javaclient.api.SupportsGrantRevoke
import com.linkedin.openhouse.spark.sql.catalyst.enums.GrantableResourceTypes
import com.linkedin.openhouse.spark.sql.catalyst.enums.GrantableResourceTypes.GrantableResourceType
import com.linkedin.openhouse.spark.sql.execution.datasources.v2.mapper.IcebergCatalogMapper
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

case class GrantRevokeStatementExec(
  isGrant: Boolean,
  resourceType: GrantableResourceType,
  catalog: TableCatalog,
  ident: Identifier,
  privilege: String,
  principal: String) extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    /** Extract {@link OpenHouseCatalog}  from {@link TableCatalog} */
    IcebergCatalogMapper.toIcebergCatalog(catalog) match {
      /** Call {@link SupportsGrantRevoke#updateTableAclPolicies} or {@link SupportsGrantRevoke#updateDatabaseAclPolicies} */
      case grantRevokableCatalog: SupportsGrantRevoke =>
        resourceType match {
          case GrantableResourceTypes.TABLE =>
            grantRevokableCatalog.updateTableAclPolicies(Spark3Util.identifierToTableIdentifier(ident), isGrant, privilege, principal)
          case GrantableResourceTypes.DATABASE =>
            grantRevokableCatalog.updateDatabaseAclPolicies(toNamespace(ident), isGrant, privilege, principal)
        }
      case _ =>
        throw new UnsupportedOperationException(s"Catalog '${catalog.name()}' does not support Grant Revoke Statements")
    }
    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"GrantRevokeStatementExec: ${catalog.name()} $isGrant $ident $privilege $principal"
  }

  private def toNamespace(ident: Identifier): Namespace = {
    val dbArray: Array[String] = ident.namespace() :+ ident.name()
    Namespace.of(dbArray:_*)
  }
}
