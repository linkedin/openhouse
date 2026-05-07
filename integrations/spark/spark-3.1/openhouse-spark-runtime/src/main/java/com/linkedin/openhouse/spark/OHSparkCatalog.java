package com.linkedin.openhouse.spark;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCapability;

/**
 * Spark catalog wrapper for OpenHouse that extends {@link SparkCatalog} and annotates every loaded
 * table with {@link TableCapability#ACCEPT_ANY_SCHEMA}.
 *
 * <p>Why {@code ACCEPT_ANY_SCHEMA}? Spark's {@code ResolveOutputRelation} analyzer rule uses the
 * session resolver (case-sensitive when {@code spark.sql.caseSensitive=true}). If a client
 * DataFrame has column {@code "id"} but the OH table stores {@code "ID"}, the write command never
 * resolves — Spark throws "Cannot find data for output column 'ID'" before OH's own server-side
 * case-insensitive schema validation runs.
 *
 * <p>Advertising {@code ACCEPT_ANY_SCHEMA} causes {@code DataSourceV2Relation.skipSchemaResolution}
 * to return {@code true}, which makes {@code V2WriteCommand.outputResolved} return {@code true}
 * immediately. {@code ResolveOutputRelation} therefore skips OH write commands, allowing {@link
 * OHWriteSchemaNormalizationRule} (a post-hoc resolution rule) to insert the necessary
 * column-renaming {@code Project} before execution.
 *
 * <p>For reads and DDL the capability has no effect; it is only consulted during write analysis.
 *
 * <p>Configuration:
 *
 * <pre>
 *   spark.sql.catalog.openhouse=com.linkedin.openhouse.spark.OHSparkCatalog
 *   spark.sql.catalog.openhouse.catalog-impl=com.linkedin.openhouse.spark.OpenHouseCatalog
 * </pre>
 */
public class OHSparkCatalog extends SparkCatalog {

  @Override
  public SparkTable loadTable(Identifier ident) throws NoSuchTableException {
    SparkTable original = super.loadTable(ident);
    return withAcceptAnySchema(original);
  }

  /**
   * Wraps a {@link SparkTable} in an anonymous subclass that adds {@link
   * TableCapability#ACCEPT_ANY_SCHEMA} to the table's capabilities.
   *
   * <p>The anonymous class delegates all other behaviour to the original table by invoking {@code
   * super} (which delegates to the underlying Iceberg table object). {@code snapshotId=null} and
   * {@code refreshEagerly=false} are the correct defaults for a standard (non-time-travel) table
   * load; the original table's Iceberg {@code Table} object is passed unchanged so all reads and
   * writes continue to use the real table state.
   */
  private SparkTable withAcceptAnySchema(SparkTable original) {
    return new SparkTable(original.table(), null /* snapshotId */, false /* refreshEagerly */) {
      @Override
      public Set<TableCapability> capabilities() {
        Set<TableCapability> caps = new HashSet<>(original.capabilities());
        caps.add(TableCapability.ACCEPT_ANY_SCHEMA);
        return Collections.unmodifiableSet(caps);
      }
    };
  }
}
