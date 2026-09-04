package com.linkedin.openhouse.internal.catalog;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Seam for publishing per-commit table stats to the Table Optimizer.
 *
 * <p>Invoked best-effort by {@link OpenHouseInternalTableOperations} immediately after a successful
 * commit, with the just-committed {@link TableMetadata} in hand (which carries the current snapshot
 * and its summary). Implementations MUST be non-blocking / fire-and-forget and MUST NOT throw: a
 * stats-publish failure must never affect the commit.
 *
 * <p>The default {@link NoOpTableStatsPublisher} does nothing, keeping the hook inert. LinkedIn
 * provides a concrete implementation (marked {@code @Primary}) that asynchronously calls the
 * optimizer stats API.
 */
public interface TableStatsPublisher {

  /**
   * Publish stats derived from a successful commit. Best-effort; implementations must not throw and
   * must not block the commit thread.
   *
   * @param tableIdentifier the committed table
   * @param committedMetadata the table metadata as committed (includes the current snapshot)
   */
  void publishOnCommit(TableIdentifier tableIdentifier, TableMetadata committedMetadata);
}
