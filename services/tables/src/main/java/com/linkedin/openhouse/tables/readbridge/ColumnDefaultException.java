package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.tables.model.TableDto;

/**
 * Write-path Type 1 / Type 2 / unusable failures. Checked so callers must handle them; the tables
 * service wraps as {@link UnsupportedClientOperationException} at the HTTP boundary (400). GET
 * {@link ReadBridgeConfigResolver#resolve} never throws this.
 */
public class ColumnDefaultException extends Exception {

  public enum Operation {
    REMOVED,
    REWRITE,
    UNUSABLE
  }

  private final Operation operation;

  public ColumnDefaultException(Operation operation, String message) {
    super(message);
    this.operation = operation;
  }

  public ColumnDefaultException(Operation operation, String message, Throwable cause) {
    super(message, cause);
    this.operation = operation;
  }

  public Operation getOperation() {
    return operation;
  }

  static ColumnDefaultException unusable(TableDto table, Throwable cause) {
    return unusable(
        table, cause.getMessage() != null ? cause.getMessage() : cause.toString(), cause);
  }

  static ColumnDefaultException unusable(TableDto table, String reason, Throwable cause) {
    return new ColumnDefaultException(
        Operation.UNUSABLE,
        String.format(
            "COLUMN_DEFAULT_UNUSABLE: OpenHouse could not validate column defaults on %s.%s"
                + " (metadata %s). Retry. If it persists, contact the OpenHouse team with the Spark"
                + " application logs. Cause: %s",
            table.getDatabaseId(), table.getTableId(), table.getTableLocation(), reason),
        cause);
  }

  /** Existing 400 mapper; call only at the service boundary. */
  public UnsupportedClientOperationException toUnsupportedClient() {
    UnsupportedClientOperationException thrown =
        new UnsupportedClientOperationException(clientOperation(), getMessage());
    thrown.initCause(this);
    return thrown;
  }

  private UnsupportedClientOperationException.Operation clientOperation() {
    switch (operation) {
      case REMOVED:
        return UnsupportedClientOperationException.Operation.COLUMN_DEFAULT_REMOVED;
      case REWRITE:
        return UnsupportedClientOperationException.Operation.COLUMN_DEFAULT_REWRITE;
      case UNUSABLE:
        return UnsupportedClientOperationException.Operation.COLUMN_DEFAULT_UNUSABLE;
      default:
        throw new IllegalArgumentException(String.valueOf(operation));
    }
  }
}
