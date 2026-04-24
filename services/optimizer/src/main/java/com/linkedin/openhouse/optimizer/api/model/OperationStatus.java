package com.linkedin.openhouse.optimizer.api.model;

/** Lifecycle states for a table operation recommendation. */
public enum OperationStatus {

  /** Recommended by the Analyzer but not yet claimed by the Scheduler. */
  PENDING,

  /** Claimed by the Scheduler; waiting for the Jobs Service to return a job ID. */
  SCHEDULING,

  /** Job submitted to the Jobs Service; the row now carries a {@code jobId}. */
  SCHEDULED,

  /**
   * Marked by the Scheduler when it detects duplicate PENDING rows for the same {@code (table_uuid,
   * operation_type)}. Only the most-recent PENDING row is claimed; older duplicates are CANCELED
   * before the claim step.
   */
  CANCELED
}
