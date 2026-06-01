package com.linkedin.openhouse.optimizer.api.spec;

/** Maintenance operation types supported by the continuous optimizer. */
public enum OperationType {
  /** Removes orphaned data files no longer referenced by table metadata. */
  ORPHAN_FILES_DELETION;

  /** Convert to the internal-model counterpart. */
  public com.linkedin.openhouse.optimizer.model.OperationTypeDto toModel() {
    return com.linkedin.openhouse.optimizer.model.OperationTypeDto.valueOf(name());
  }

  /** Build the api-layer enum from the internal-model counterpart. */
  public static OperationType fromModel(com.linkedin.openhouse.optimizer.model.OperationTypeDto v) {
    return v == null ? null : OperationType.valueOf(v.name());
  }
}
