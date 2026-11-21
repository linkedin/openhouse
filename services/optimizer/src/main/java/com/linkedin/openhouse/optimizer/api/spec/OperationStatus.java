package com.linkedin.openhouse.optimizer.api.spec;

/** Lifecycle states for a table operation recommendation. */
public enum OperationStatus {

  /** Recommended by the Analyzer but not yet claimed by the Scheduler. */
  PENDING,

  /** Claimed by the Scheduler; {@code jobId} is set once the Jobs Service accepts the job. */
  SCHEDULED,

  /**
   * Marked by the Scheduler when it detects duplicate PENDING rows for the same {@code (table_uuid,
   * operation_type)}. Only the most-recent PENDING row is claimed; older duplicates are CANCELED
   * before the claim step.
   */
  CANCELED;

  /** Convert to the internal-model counterpart. */
  public com.linkedin.openhouse.optimizer.model.OperationStatusDto toModel() {
    return com.linkedin.openhouse.optimizer.model.OperationStatusDto.valueOf(name());
  }

  /** Build the api-layer enum from the internal-model counterpart. */
  public static OperationStatus fromModel(
      com.linkedin.openhouse.optimizer.model.OperationStatusDto v) {
    return v == null ? null : OperationStatus.valueOf(v.name());
  }
}
