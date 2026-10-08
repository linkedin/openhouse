package com.linkedin.openhouse.optimizer.db;

import com.linkedin.openhouse.optimizer.model.HistoryStatusDto;

/**
 * DB-layer enum for the {@code status} column of {@code table_operations_history}.
 *
 * <p>Converts to and from its model/ counterpart; no references to api/ types.
 */
public enum HistoryStatus {

  /** The Spark job for this operation completed successfully. */
  SUCCESS,

  /** The Spark job for this operation failed. */
  FAILED;

  /** Convert to the internal-model counterpart. */
  public HistoryStatusDto toModel() {
    return HistoryStatusDto.valueOf(name());
  }

  /** Build the DB-layer enum from the internal-model counterpart. */
  public static HistoryStatus fromModel(HistoryStatusDto v) {
    return v == null ? null : HistoryStatus.valueOf(v.name());
  }
}
