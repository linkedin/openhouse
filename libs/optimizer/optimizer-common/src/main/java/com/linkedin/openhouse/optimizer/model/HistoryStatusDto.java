package com.linkedin.openhouse.optimizer.model;

/**
 * Internal lifecycle outcomes for a completed operation. Mirrors the values written to {@code
 * table_operations_history.status}; parsed at the boundary so callers switch on a typed value
 * instead of comparing strings.
 *
 * <p>Intentionally separate from the wire-API and DB representations.
 */
public enum HistoryStatusDto {

  /** The operation completed successfully. */
  SUCCESS,

  /** The operation failed. */
  FAILED,

  /**
   * A submitted job that never reported a terminal result and has passed the stale-timeout. Treated
   * as a non-success (a failure) by the circuit breaker's streak count.
   */
  TIMED_OUT;

  /** Convert to the DB-layer counterpart. */
  public com.linkedin.openhouse.optimizer.db.HistoryStatus toDb() {
    return com.linkedin.openhouse.optimizer.db.HistoryStatus.valueOf(name());
  }

  /** Build the internal-model enum from the DB-layer counterpart. */
  public static HistoryStatusDto fromDb(com.linkedin.openhouse.optimizer.db.HistoryStatus v) {
    return v == null ? null : HistoryStatusDto.valueOf(v.name());
  }
}
