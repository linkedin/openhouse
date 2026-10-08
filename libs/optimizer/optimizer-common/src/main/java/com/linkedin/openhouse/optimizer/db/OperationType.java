package com.linkedin.openhouse.optimizer.db;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;

/**
 * DB-layer enum for the operation types persisted in {@code table_operations.operation_type} and
 * {@code table_operations_history.operation_type}.
 *
 * <p>Converts to and from its model/ counterpart; no references to api/ types. JPA binds this via
 * {@code @Enumerated(EnumType.STRING)}.
 */
public enum OperationType {

  /** Removes orphaned data files no longer referenced by table metadata. */
  ORPHAN_FILES_DELETION,

  /** Collects per-table snapshot/size statistics used to drive optimizer decisions. */
  TABLE_STATS_COLLECTION;

  /** Convert to the internal-model counterpart. */
  public OperationTypeDto toModel() {
    return OperationTypeDto.valueOf(name());
  }

  /** Build the DB-layer enum from the internal-model counterpart. */
  public static OperationType fromModel(OperationTypeDto v) {
    return v == null ? null : OperationType.valueOf(v.name());
  }
}
