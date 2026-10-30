package com.linkedin.openhouse.javaclient.precommit;

import java.util.Objects;
import java.util.Optional;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * What a {@link PreCommitHook} is told about the commit it is checking.
 *
 * <p>Carries the values {@code doCommit} has already worked out, so a hook never has to guess at
 * them. {@link #getOperation()} in particular is the branch {@code doCommit} is about to take, not
 * a second opinion derived from the metadata.
 */
public final class PreCommitHookContext {

  private final String cluster;
  private final TableIdentifier tableIdentifier;
  private final PreCommitHookOperation operation;
  private final TableMetadata base;
  private final TableMetadata metadata;
  private final boolean replaceTransaction;

  public PreCommitHookContext(
      String cluster,
      TableIdentifier tableIdentifier,
      TableMetadata base,
      TableMetadata metadata,
      boolean replaceTransaction,
      boolean metadataUpdated,
      boolean snapshotsUpdated) {
    this.cluster = Objects.requireNonNull(cluster, "cluster");
    this.tableIdentifier = Objects.requireNonNull(tableIdentifier, "tableIdentifier");
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    this.base = base;
    this.replaceTransaction = replaceTransaction;
    this.operation =
        PreCommitHookOperation.of(base, replaceTransaction, metadataUpdated, snapshotsUpdated);
  }

  public String getCluster() {
    return cluster;
  }

  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  /** The branch {@code doCommit} is about to take. */
  public PreCommitHookOperation getOperation() {
    return operation;
  }

  /** The table as it is now. Empty when the table is being created. */
  public Optional<TableMetadata> getBase() {
    return Optional.ofNullable(base);
  }

  /** The table as it will be if the commit goes through. */
  public TableMetadata getMetadata() {
    return metadata;
  }

  /**
   * Whether the writer asked for a replace.
   *
   * <p>Not the same as {@link PreCommitHookOperation#REPLACE}, which also needs a base to replace.
   * {@code OpenHouseCatalog} only sets this flag when the table already exists, so the two agree
   * there: a {@code CREATE OR REPLACE TABLE} of a table that does not exist yet is routed to a
   * plain create and the flag stays false. The operation is derived from both so that an integrator
   * setting the flag some other way still cannot produce a REPLACE with nothing to replace.
   */
  public boolean isReplaceTransaction() {
    return replaceTransaction;
  }

  @Override
  public String toString() {
    return "PreCommitHookContext{cluster="
        + cluster
        + ", table="
        + tableIdentifier
        + ", operation="
        + operation
        + ", replaceTransaction="
        + replaceTransaction
        + "}";
  }
}
