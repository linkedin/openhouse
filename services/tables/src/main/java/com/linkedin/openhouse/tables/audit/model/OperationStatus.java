package com.linkedin.openhouse.tables.audit.model;

/** The status for a specific table or view operation. */
public enum OperationStatus {
  FAILED,
  SUCCESS,

  /**
   * The write may have succeeded or failed. The client must query the server to verify the view's
   * state before retrying.
   */
  UNKNOWN
}
