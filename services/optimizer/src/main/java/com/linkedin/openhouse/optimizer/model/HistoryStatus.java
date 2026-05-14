package com.linkedin.openhouse.optimizer.model;

/**
 * Internal lifecycle outcomes for a completed operation. Mirrors the values written to {@code
 * table_operations_history.status}; parsed at the boundary so callers switch on a typed value
 * instead of comparing strings.
 *
 * <p>Intentionally separate from the wire-API and DB representations.
 */
public enum HistoryStatus {

  /** The operation completed successfully. */
  SUCCESS,

  /** The operation failed. */
  FAILED
}
