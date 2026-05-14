package com.linkedin.openhouse.optimizer.api.model;

/** Maintenance operation types supported by the continuous optimizer. */
public enum OperationType {
  /** Removes orphaned data files no longer referenced by table metadata. */
  ORPHAN_FILES_DELETION;

  /** Convert to the internal-model counterpart. */
  public com.linkedin.openhouse.optimizer.model.OperationType toModel() {
    return com.linkedin.openhouse.optimizer.model.OperationType.valueOf(name());
  }

  /** Build the api-layer enum from the internal-model counterpart. */
  public static OperationType fromModel(com.linkedin.openhouse.optimizer.model.OperationType v) {
    return v == null ? null : OperationType.valueOf(v.name());
  }
}
