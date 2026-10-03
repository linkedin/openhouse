package com.linkedin.openhouse.tables.audit.model;

/** The status for a specific table or view operation. */
public enum OperationStatus {
  FAILED,
  SUCCESS,

  /**
   * The write's publication outcome is unacknowledged: it may have succeeded or failed. Recorded
   * exactly once per ambiguous commit; never retried or re-classified as SUCCESS/FAILED.
   */
  UNKNOWN
}
