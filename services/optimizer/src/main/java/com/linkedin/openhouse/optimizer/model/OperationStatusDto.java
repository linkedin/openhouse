package com.linkedin.openhouse.optimizer.model;

/**
 * Internal lifecycle states for an operation. The analyzer writes {@link #PENDING}; the scheduler
 * transitions through {@link #SCHEDULING} and {@link #SCHEDULED}. {@link #CANCELED} marks
 * deduplicated PENDING rows.
 *
 * <p>Intentionally separate from the wire-API and DB representations.
 */
public enum OperationStatusDto {

  /** Analyzer has written the row; not yet claimed by the scheduler. */
  PENDING,

  /** Scheduler has claimed the row and is launching a job; jobId not yet recorded. */
  SCHEDULING,

  /** Job has been submitted to the Jobs Service; the row carries a {@code jobId}. */
  SCHEDULED,

  /** Scheduler marked this row as a duplicate of another PENDING row; not claimable. */
  CANCELED
}
