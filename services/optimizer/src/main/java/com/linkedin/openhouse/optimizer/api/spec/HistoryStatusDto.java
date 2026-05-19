package com.linkedin.openhouse.optimizer.api.spec;

/** Terminal states for a completed Spark maintenance job. */
public enum HistoryStatusDto {

  /** The Spark job for this operation completed successfully. */
  SUCCESS,

  /** The Spark job for this operation failed. */
  FAILED;

  /** Convert to the internal-model counterpart. */
  public com.linkedin.openhouse.optimizer.model.HistoryStatus toModel() {
    return com.linkedin.openhouse.optimizer.model.HistoryStatus.valueOf(name());
  }

  /** Build the api-layer enum from the internal-model counterpart. */
  public static HistoryStatusDto fromModel(com.linkedin.openhouse.optimizer.model.HistoryStatus v) {
    return v == null ? null : HistoryStatusDto.valueOf(v.name());
  }
}
