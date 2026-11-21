package com.linkedin.openhouse.optimizer.db;

/**
 * DB-layer enum for the {@code status} column of {@code table_operations}.
 *
 * <p>Self-contained: no references to api/ or model/ types.
 */
public enum OperationStatus {

  /** Analyzer has written the row; not yet claimed by the scheduler. */
  PENDING,

  /** Scheduler has claimed the row; {@code jobId} is set once the Jobs Service accepts the job. */
  SCHEDULED,

  /** Scheduler marked this row as a duplicate of another PENDING row; not claimable. */
  CANCELED
}
