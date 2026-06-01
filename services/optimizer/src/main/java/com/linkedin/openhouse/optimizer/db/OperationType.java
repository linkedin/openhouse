package com.linkedin.openhouse.optimizer.db;

/**
 * DB-layer enum for the operation types persisted in {@code table_operations.operation_type} and
 * {@code table_operations_history.operation_type}.
 *
 * <p>Self-contained: no references to api/ or model/ types. JPA binds this via
 * {@code @Enumerated(EnumType.STRING)}.
 */
public enum OperationType {

  /** Removes orphaned data files no longer referenced by table metadata. */
  ORPHAN_FILES_DELETION
}
