package com.linkedin.openhouse.internal.catalog;

/**
 * Seam for publishing per-commit table stats to the Table Optimizer.
 *
 * <p>Invoked best-effort by {@link OpenHouseInternalTableOperations} immediately after a successful
 * commit, with the already-extracted {@link CommitStats}. Implementations MUST be non-blocking /
 * fire-and-forget and MUST NOT throw: a stats-publish failure must never affect the commit.
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
   * @param commitStats neutral, already-extracted commit stats
   */
  void publishOnCommit(CommitStats commitStats);
}
