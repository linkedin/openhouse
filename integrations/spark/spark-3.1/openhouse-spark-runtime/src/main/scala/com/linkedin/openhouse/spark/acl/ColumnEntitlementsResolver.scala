package com.linkedin.openhouse.spark.acl

import java.util.concurrent.ConcurrentHashMap

import com.linkedin.openhouse.javaclient.api.SupportsColumnEntitlements
import com.linkedin.openhouse.spark.sql.execution.datasources.v2.mapper.IcebergCatalogMapper
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, TableCatalog}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Resolves which columns of an OpenHouse table the current principal must not read.
 *
 * The catalog is the policy decision point: it holds both the policy tags attached to columns and
 * the grants held by principals, and returns the already-resolved answer. Spark only applies it.
 */
private[acl] class ColumnEntitlementsResolver(cacheTtlMillis: Long) {

  private val log = LoggerFactory.getLogger(classOf[ColumnEntitlementsResolver])

  private val cache = new ConcurrentHashMap[String, ColumnEntitlementsResolver.CacheEntry]()

  /**
   * Restricted columns for `identifier`, or an empty set when the table carries no column tags.
   *
   * Tables without tags are answered locally from the table properties, so the common case costs
   * no catalog round trip and deployments whose catalog predates column ACLs keep working.
   */
  def restrictedColumns(
      catalog: CatalogPlugin,
      identifier: Identifier,
      table: Table): Set[String] = {
    if (!ColumnEntitlementsResolver.isEligible(table)) {
      Set.empty
    } else {
      val key = s"${catalog.name()}.${identifier.toString}"
      val now = System.currentTimeMillis()
      Option(cache.get(key)).filter(_.expiresAt > now) match {
        case Some(entry) => entry.restrictedColumns
        case None =>
          val resolved = fetch(catalog, identifier)
          if (cacheTtlMillis > 0) {
            cache.put(key, ColumnEntitlementsResolver.CacheEntry(resolved, now + cacheTtlMillis))
          }
          resolved
      }
    }
  }

  /**
   * Asks the catalog for the caller's entitlements.
   *
   * Failures are propagated rather than swallowed: the table is known to carry restricted columns,
   * so being unable to establish what the caller may read must not result in reading everything.
   */
  private def fetch(catalog: CatalogPlugin, identifier: Identifier): Set[String] = {
    val icebergCatalog = catalog match {
      case tableCatalog: TableCatalog => IcebergCatalogMapper.toIcebergCatalog(tableCatalog)
      case _ => null
    }
    icebergCatalog match {
      case entitlements: SupportsColumnEntitlements =>
        val dto =
          entitlements.getColumnEntitlements(Spark3Util.identifierToTableIdentifier(identifier))
        val restricted =
          Option(dto.getRestrictedColumns).map(_.asScala.toSet).getOrElse(Set.empty[String])
        if (restricted.nonEmpty) {
          log.info(
            s"OpenHouse column ACL restricts columns [${restricted.mkString(", ")}] on table " +
              s"$identifier for the current principal")
        }
        restricted
      case _ =>
        throw new IllegalStateException(
          s"Table '$identifier' carries column policy tags but catalog '${catalog.name()}' cannot " +
            "resolve column entitlements, so restricted columns cannot be masked")
    }
  }
}

private[acl] object ColumnEntitlementsResolver {

  private val PoliciesProperty = "policies"

  private val TableIdProperty = "openhouse.tableId"

  private val ColumnTagsField = "columnTags"

  case class CacheEntry(restrictedColumns: Set[String], expiresAt: Long)

  /**
   * Whether the table is an OpenHouse table that declares at least one column policy tag.
   *
   * Restricting this to OpenHouse tables keeps tables from other catalogs, which have no notion of
   * column entitlements, out of the fail closed path below.
   *
   * The tag check is deliberately textual and errs towards `true`: a false positive only costs a
   * catalog call, whereas a false negative would skip masking altogether.
   */
  def isEligible(table: Table): Boolean = {
    val properties = Option(table.properties())
    properties.exists(_.containsKey(TableIdProperty)) && properties
      .flatMap(props => Option(props.get(PoliciesProperty)))
      .exists { value =>
        val index = value.indexOf(ColumnTagsField)
        if (index < 0) {
          false
        } else {
          val remainder = value.substring(index + ColumnTagsField.length).replaceAll("[\"\\s:]", "")
          !remainder.startsWith("{}") && !remainder.startsWith("null")
        }
      }
  }
}
