package com.linkedin.openhouse.internal.catalog;

/**
 * A best-effort unit of work executed after a table is successfully dropped.
 *
 * <p>The drop analogue of {@link PostCommitOperation}: it lets arbitrary business logic (e.g.
 * purging an external system's per-table state) run after a drop without the catalog knowing
 * anything about that logic. All discovered beans are collected by {@link
 * PostCommitOperationRunner}, which owns the same bounded, async, timeout-guarded execution
 * guarantees it applies to post-commit operations.
 *
 * <p>Contract for implementors:
 *
 * <ul>
 *   <li><b>Best effort.</b> The drop has already durably succeeded before an operation runs. An
 *       operation failing, timing out, or being dropped must never affect drop correctness.
 *   <li><b>Bounded &amp; interruptible.</b> Operations run on a bounded pool with a hard per-op
 *       timeout. Long/blocking work must respond to thread interruption so it can be cancelled.
 *   <li><b>Self-contained.</b> Extract only what is needed from the {@link PostDropContext}; do not
 *       assume anything about other operations or ordering.
 * </ul>
 */
public interface PostDropOperation {

  /**
   * Short, stable name used to tag metrics and logs for this operation. Must be low-cardinality
   * (e.g. "table-stats-delete").
   */
  String getName();

  /**
   * Executes the operation. Implementations may throw; the runner isolates and records failures.
   *
   * @param context the just-dropped table's identity and UUID
   * @throws Exception any failure; treated as best-effort and recorded, never propagated to the
   *     caller that performed the drop
   */
  void execute(PostDropContext context) throws Exception;
}
