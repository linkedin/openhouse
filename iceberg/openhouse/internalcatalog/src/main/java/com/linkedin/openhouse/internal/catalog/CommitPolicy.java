package com.linkedin.openhouse.internal.catalog;

/**
 * Business rules a table commit must pass, supplied by the service that hosts this catalog. {@link
 * OpenHouseInternalTableOperations} checks every commit after its stale-base check and before
 * writing anything.
 */
@FunctionalInterface
public interface CommitPolicy {

  /** Accepts every commit. */
  CommitPolicy NONE = commit -> {};

  /**
   * Rejects {@code commit} by throwing the error the caller reports; the catalog rethrows it
   * unchanged.
   */
  void check(TableCommit commit);
}
