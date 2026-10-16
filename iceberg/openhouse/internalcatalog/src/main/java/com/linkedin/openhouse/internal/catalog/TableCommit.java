package com.linkedin.openhouse.internal.catalog;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import lombok.Value;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

/** One table commit, as {@link CommitPolicy} judges it. */
@Value
public class TableCommit {

  /** The committed table. Its namespace is the database. */
  TableIdentifier tableIdentifier;

  /** The table's current metadata; empty when the commit creates the table. */
  Optional<TableMetadata> base;

  /** The schema the commit leaves. */
  Schema schema;

  /**
   * The properties the commit leaves, after transient overrides are restored and OpenHouse's own
   * table property rules applied. doCommit's commit-only keys, such as {@link
   * CatalogConstants#SNAPSHOTS_JSON_KEY}, are still present.
   */
  ImmutableMap<String, String> properties;

  /** Whether the commit puts snapshots, on any branch, or replaces the table, staged or not. */
  boolean snapshotCommit;

  /**
   * The commit doCommit is about to write. {@code properties} still carry its commit-only keys,
   * which tell whether it puts snapshots or replaces the table.
   */
  static TableCommit of(
      TableIdentifier tableIdentifier,
      TableMetadata base,
      Schema schema,
      Map<String, String> properties) {
    return new TableCommit(
        tableIdentifier,
        Optional.ofNullable(base),
        schema,
        ImmutableMap.copyOf(properties),
        Boolean.parseBoolean(properties.get(CatalogConstants.IS_REPLACE_COMMIT_KEY))
            || Boolean.parseBoolean(properties.get(CatalogConstants.IS_STAGE_REPLACE_KEY))
            || properties.containsKey(CatalogConstants.SNAPSHOTS_JSON_KEY));
  }
}
