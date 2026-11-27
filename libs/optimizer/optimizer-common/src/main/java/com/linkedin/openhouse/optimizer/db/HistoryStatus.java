package com.linkedin.openhouse.optimizer.db;

/**
 * DB-layer enum for the {@code status} column of {@code table_operations_history}.
 *
 * <p>Self-contained: no references to api/ or model/ types.
 */
public enum HistoryStatus {

  /** The Spark job for this operation completed successfully. */
  SUCCESS,

  /** The Spark job for this operation failed. */
  FAILED
}
