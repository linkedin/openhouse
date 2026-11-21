package com.linkedin.openhouse.optimizer.model;

/**
 * Internal lifecycle states for an operation. The analyzer writes {@link #PENDING}; the scheduler
 * sets {@link #SCHEDULED} when it claims the row. {@link #CANCELED} marks deduplicated PENDING
 * rows.
 *
 * <p>Intentionally separate from the wire-API and DB representations.
 */
public enum OperationStatusDto {

  /** Analyzer has written the row; not yet claimed by the scheduler. */
  PENDING,

  /** Scheduler has claimed the row; {@code jobId} is set once the Jobs Service accepts the job. */
  SCHEDULED,

  /** Scheduler marked this row as a duplicate of another PENDING row; not claimable. */
  CANCELED;

  /** Convert to the DB-layer counterpart. */
  public com.linkedin.openhouse.optimizer.db.OperationStatus toDb() {
    return com.linkedin.openhouse.optimizer.db.OperationStatus.valueOf(name());
  }

  /** Build the internal-model enum from the DB-layer counterpart. */
  public static OperationStatusDto fromDb(com.linkedin.openhouse.optimizer.db.OperationStatus v) {
    return v == null ? null : OperationStatusDto.valueOf(v.name());
  }
}
