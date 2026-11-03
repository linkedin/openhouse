package com.linkedin.openhouse.internal.catalog;

/**
 * A best-effort unit of work executed after a table commit succeeds.
 *
 * <p>Implementations plug arbitrary business logic (e.g. publishing commit stats) into the commit
 * path without the catalog knowing anything about that logic. All discovered beans are collected by
 * {@link PostCommitOperationRunner}, which owns the execution guarantees.
 *
 * <p>Contract for implementors:
 *
 * <ul>
 *   <li><b>Best effort.</b> The commit has already durably succeeded before an operation runs. An
 *       operation failing, timing out, or being dropped must never affect commit correctness.
 *   <li><b>Bounded &amp; interruptible.</b> Operations run on a bounded pool with a hard per-op
 *       timeout. Long/blocking work must respond to thread interruption so it can be cancelled.
 *   <li><b>Self-contained.</b> Extract only what is needed from the {@link PostCommitContext}; do
 *       not assume anything about other operations or ordering.
 * </ul>
 */
public interface PostCommitOperation {

  /**
   * Short, stable name used to tag metrics and logs for this operation. Must be low-cardinality
   * (e.g. "commit-stats-publish").
   */
  String getName();

  /**
   * Executes the operation. Implementations may throw; the runner isolates and records failures.
   *
   * @param context the just-committed table identity and metadata
   * @throws Exception any failure; treated as best-effort and recorded, never propagated to the
   *     committer
   */
  void execute(PostCommitContext context) throws Exception;
}
