package com.linkedin.openhouse.optimizer.api.spec;

/** Terminal states for a completed Spark maintenance job. */
public enum HistoryStatus {

  /** The Spark job for this operation completed successfully. */
  SUCCESS,

  /** The Spark job for this operation failed. */
  FAILED,

  /**
   * The operation was submitted but never reported a terminal result and timed out. Recorded by the
   * analyzer when it supersedes the dead op.
   */
  TIMED_OUT;

  /** Convert to the internal-model counterpart. */
  public com.linkedin.openhouse.optimizer.model.HistoryStatusDto toModel() {
    return com.linkedin.openhouse.optimizer.model.HistoryStatusDto.valueOf(name());
  }

  /** Build the api-layer enum from the internal-model counterpart. */
  public static HistoryStatus fromModel(com.linkedin.openhouse.optimizer.model.HistoryStatusDto v) {
    return v == null ? null : HistoryStatus.valueOf(v.name());
  }
}
