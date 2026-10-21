package com.linkedin.openhouse.internal.catalog;

import com.linkedin.openhouse.common.exception.CommitRejectedException;

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
   * Accepts {@code commit}, or rejects it by throwing; the catalog rethrows the error unchanged.
   *
   * @throws CommitRejectedException if {@code commit} breaks this policy, as the subclass that
   *     names the broken rule
   */
  void check(TableCommit commit);
}
