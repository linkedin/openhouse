package com.linkedin.openhouse.optimizer.api.spec;

/** Maintenance operation types supported by the continuous optimizer. */
public enum OperationTypeDto {
  /** Removes orphaned data files no longer referenced by table metadata. */
  ORPHAN_FILES_DELETION;

  /** Convert to the internal-model counterpart. */
  public com.linkedin.openhouse.optimizer.model.OperationType toModel() {
    return com.linkedin.openhouse.optimizer.model.OperationType.valueOf(name());
  }

  /** Build the api-layer enum from the internal-model counterpart. */
  public static OperationTypeDto fromModel(com.linkedin.openhouse.optimizer.model.OperationType v) {
    return v == null ? null : OperationTypeDto.valueOf(v.name());
  }
}
