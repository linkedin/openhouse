package com.linkedin.openhouse.optimizer.api.model;

/** Terminal states for a completed Spark maintenance job. */
public enum HistoryStatus {

  /** The Spark job for this operation completed successfully. */
  SUCCESS,

  /** The Spark job for this operation failed. */
  FAILED;

  /** Convert to the internal-model counterpart. */
  public com.linkedin.openhouse.optimizer.model.HistoryStatus toModel() {
    return com.linkedin.openhouse.optimizer.model.HistoryStatus.valueOf(name());
  }

  /** Build the api-layer enum from the internal-model counterpart. */
  public static HistoryStatus fromModel(com.linkedin.openhouse.optimizer.model.HistoryStatus v) {
    return v == null ? null : HistoryStatus.valueOf(v.name());
  }
}
