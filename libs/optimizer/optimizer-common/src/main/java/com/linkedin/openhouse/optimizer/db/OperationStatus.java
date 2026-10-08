package com.linkedin.openhouse.optimizer.db;

import com.linkedin.openhouse.optimizer.model.OperationStatusDto;

/**
 * DB-layer enum for the {@code status} column of {@code table_operations}.
 *
 * <p>Converts to and from its model/ counterpart; no references to api/ types.
 */
public enum OperationStatus {

  /** Analyzer has written the row; not yet claimed by the scheduler. */
  PENDING,

  /** Scheduler has claimed the row and is launching a job; jobId not yet recorded. */
  SCHEDULING,

  /** Job has been submitted to the Jobs Service; the row carries a {@code jobId}. */
  SCHEDULED,

  /** Scheduler marked this row as a duplicate of another PENDING row; not claimable. */
  CANCELED;

  /** Convert to the internal-model counterpart. */
  public OperationStatusDto toModel() {
    return OperationStatusDto.valueOf(name());
  }

  /** Build the DB-layer enum from the internal-model counterpart. */
  public static OperationStatus fromModel(OperationStatusDto v) {
    return v == null ? null : OperationStatus.valueOf(v.name());
  }
}
