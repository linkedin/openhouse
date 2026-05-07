package com.linkedin.openhouse.spark;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;

/**
 * OpenHouse catalog extension for Spark 3.5 / Iceberg 1.5. Overrides {@link
 * SparkCatalog#loadTable(Identifier)} to advertise {@link TableCapability#ACCEPT_ANY_SCHEMA} on
 * every OpenHouse table. This prevents Spark's {@code ResolveOutputRelation} from throwing at
 * analysis time when {@code caseSensitive=true} and the source column casing differs from the
 * stored column name. The companion rule {@link
 * com.linkedin.openhouse.spark.extensions.OHWriteSchemaNormalizationRule} runs as a post-hoc
 * resolution rule and applies the necessary column renaming / casting that {@code
 * ResolveOutputRelation} would otherwise have done.
 */
public class OHSparkCatalog extends SparkCatalog {

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    Table original = super.loadTable(ident);
    if (original instanceof SparkTable) {
      return withAcceptAnySchema((SparkTable) original);
    }
    return original;
  }

  private SparkTable withAcceptAnySchema(SparkTable original) {
    // SparkTable carries a branch field (set when loading branch-qualified identifiers like
    // "table.branch_feature_a"). We must use the SparkTable(Table, String, boolean) constructor
    // when a branch is present so that newWriteBuilder() targets the correct branch.
    // Using SparkTable(Table, Long, boolean) with snapshotId=null would silently drop the branch
    // and cause all branch writes to land on the main table instead.
    String branch = original.branch();
    if (branch != null) {
      return new SparkTable(original.table(), branch, false /* refreshEagerly */) {
        @Override
        public Set<TableCapability> capabilities() {
          Set<TableCapability> caps = new HashSet<>(original.capabilities());
          caps.add(TableCapability.ACCEPT_ANY_SCHEMA);
          return Collections.unmodifiableSet(caps);
        }
      };
    }
    return new SparkTable(original.table(), original.snapshotId(), false /* refreshEagerly */) {
      @Override
      public Set<TableCapability> capabilities() {
        Set<TableCapability> caps = new HashSet<>(original.capabilities());
        caps.add(TableCapability.ACCEPT_ANY_SCHEMA);
        return Collections.unmodifiableSet(caps);
      }
    };
  }
}
