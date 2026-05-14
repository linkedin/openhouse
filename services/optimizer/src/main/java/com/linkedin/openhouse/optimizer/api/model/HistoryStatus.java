package com.linkedin.openhouse.optimizer.api.model;

/** Terminal states for a completed Spark maintenance job. */
public enum HistoryStatus {

  /** The Spark job for this operation completed successfully. */
  SUCCESS,

  /** The Spark job for this operation failed. */
  FAILED
}
