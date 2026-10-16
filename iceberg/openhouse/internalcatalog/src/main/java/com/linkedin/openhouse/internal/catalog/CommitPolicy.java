package com.linkedin.openhouse.internal.catalog;

import com.linkedin.openhouse.internal.catalog.exception.CommitRejectedException;

/**
 * Business rules a table commit must pass, supplied by the service that hosts this catalog. {@link
 * OpenHouseInternalTableOperations} checks every commit after its stale-base check and before
 * writing anything.
 */
@FunctionalInterface
public interface CommitPolicy {

  /** Accepts every commit. */
  CommitPolicy NONE = commit -> {};

  /** @throws CommitRejectedException to reject the commit with the error it carries */
  void check(TableCommit commit) throws CommitRejectedException;
}
