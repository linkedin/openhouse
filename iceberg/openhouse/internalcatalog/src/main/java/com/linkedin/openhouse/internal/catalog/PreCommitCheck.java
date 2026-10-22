package com.linkedin.openhouse.internal.catalog;

import com.linkedin.openhouse.common.exception.CommitRejectedException;

/**
 * A business rule a table commit must pass before anything is written. The pre-commit counterpart
 * of {@link PostCommitOperation}: implementations plug rules into the commit path without the
 * catalog knowing them. All discovered beans are collected by {@link PreCommitCheckRunner}, which
 * runs them in {@code @Order} and owns their metrics and error handling.
 *
 * <p>Contract for implementors:
 *
 * <ul>
 *   <li><b>Check only.</b> Accept by returning; reject by throwing a {@link
 *       CommitRejectedException} whose message names the broken rule. Never change the commit.
 *   <li><b>Synchronous and bounded.</b> Runs on the committing thread, inside the commit. A check
 *       that calls another service owns its timeout and reports that service being unavailable as a
 *       server error, not as a rejection.
 *   <li><b>Current base only.</b> Runs after the catalog's stale-base check and table property
 *       rules, so it judges only commits built on the table's current version, and sees the
 *       properties the commit leaves.
 * </ul>
 */
public interface PreCommitCheck {

  /**
   * Short, stable name used to tag metrics and logs for this check. Must be low-cardinality (e.g.
   * "column-default-opt-in").
   */
  String getName();

  /**
   * Accepts {@code context}'s commit, or rejects it by throwing.
   *
   * @throws CommitRejectedException if the commit breaks this check's rule
   */
  void check(PreCommitContext context);
}
