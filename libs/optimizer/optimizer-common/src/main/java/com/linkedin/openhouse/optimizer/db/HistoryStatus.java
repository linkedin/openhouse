package com.linkedin.openhouse.optimizer.db;

/**
 * DB-layer enum for the {@code status} column of {@code table_operations_history}.
 *
 * <p>Self-contained: no references to api/ or model/ types.
 */
public enum HistoryStatus {

  /** The Spark job for this operation completed successfully. */
  SUCCESS,

  /** The Spark job for this operation failed. */
  FAILED,

  /**
   * The operation was submitted (a job was launched) but never reported a terminal result and has
   * passed the stale-timeout — a silent failure recorded by the analyzer when it supersedes the
   * dead op. Counts as a non-success for the circuit breaker.
   */
  TIMED_OUT
}
