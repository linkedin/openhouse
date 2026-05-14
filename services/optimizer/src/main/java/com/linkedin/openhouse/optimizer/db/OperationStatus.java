package com.linkedin.openhouse.optimizer.db;

/**
 * DB-layer enum for the {@code status} column of {@code table_operations}.
 *
 * <p>Self-contained: no references to api/ or model/ types.
 */
public enum OperationStatus {
  PENDING,
  SCHEDULING,
  SCHEDULED,
  CANCELED
}
