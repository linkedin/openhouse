package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.tables.model.TableDto;

/**
 * Column-default failures on reads and writes. Checked so each boundary chooses the HTTP mapping:
 * writes use {@link #toUnsupportedClient()} (400) and GET uses {@link #toServerError()} (500).
 * Deployment sources report why a declared default is unusable with a {@link Reason}.
 */
public class ColumnDefaultException extends Exception {

  public enum Operation {
    REMOVED,
    REWRITE,
    UNUSABLE
  }

  /** Why a {@link ColumnDefaultsSource} cannot apply a declared default. */
  public enum Reason {
    /** The declaring schema, or the table's Iceberg schema, cannot be read. */
    INVALID_SCHEMA,
    /** The declared default is malformed for its declared type. */
    INVALID_VALUE,
    /** The default cannot bind to the column type, including ambiguous units or scale. */
    TYPE_MISMATCH,
    /** The value overflows or falls outside the column type's range. */
    OUT_OF_RANGE,
    /** A valid default OpenHouse cannot apply, such as a default inside a collection. */
    UNSUPPORTED,
    /** The source failed unexpectedly. */
    INTERNAL
  }

  private final Operation operation;
  private final Reason reason;
  private final Integer fieldId;
  private final String columnPath;
  private final String sourceType;
  private final String targetType;

  public ColumnDefaultException(Operation operation, String message) {
    this(operation, message, null);
  }

  public ColumnDefaultException(Operation operation, String message, Throwable cause) {
    super(message, cause);
    this.operation = operation;
    this.reason = null;
    this.fieldId = null;
    this.columnPath = null;
    this.sourceType = null;
    this.targetType = null;
  }

  /** Source-reported failure without column context. */
  public ColumnDefaultException(Reason reason, TableDto table, Throwable cause) {
    this(reason, table, null, null, null, null, cause);
  }

  /**
   * Source-reported failure for one column; types are labels, never the default value. Tolerates
   * missing context so constructing the failure cannot mask its cause.
   */
  public ColumnDefaultException(
      Reason reason,
      TableDto table,
      Integer fieldId,
      String columnPath,
      String sourceType,
      String targetType,
      Throwable cause) {
    super(unusableMessage(reason, table, fieldId, columnPath, sourceType, targetType), cause);
    this.operation = Operation.UNUSABLE;
    this.reason = reason;
    this.fieldId = fieldId;
    this.columnPath = columnPath;
    this.sourceType = sourceType;
    this.targetType = targetType;
  }

  public Operation getOperation() {
    return operation;
  }

  /** Source-reported reason; null when OpenHouse raised the failure itself. */
  public Reason getReason() {
    return reason;
  }

  public Integer getFieldId() {
    return fieldId;
  }

  public String getColumnPath() {
    return columnPath;
  }

  public String getSourceType() {
    return sourceType;
  }

  public String getTargetType() {
    return targetType;
  }

  private static String unusableMessage(
      Reason reason,
      TableDto table,
      Integer fieldId,
      String columnPath,
      String sourceType,
      String targetType) {
    StringBuilder detail = new StringBuilder(String.valueOf(reason));
    if (columnPath != null) {
      detail.append(", column ").append(columnPath);
    }
    if (fieldId != null) {
      detail.append(", field ID ").append(fieldId);
    }
    if (sourceType != null) {
      detail.append(", source type ").append(sourceType);
    }
    if (targetType != null) {
      detail.append(", target type ").append(targetType);
    }
    String tableName =
        table == null ? "an unknown table" : table.getDatabaseId() + "." + table.getTableId();
    return String.format(
        "COLUMN_DEFAULT_UNUSABLE: OpenHouse cannot apply the column defaults declared on %s (%s). %s",
        tableName, detail, remediation(reason));
  }

  private static String remediation(Reason reason) {
    if (reason == Reason.INTERNAL) {
      return "Retry. If it persists, contact the OpenHouse team.";
    }
    if (reason == Reason.UNSUPPORTED) {
      return "Remove or change the declared default; OpenHouse cannot apply it.";
    }
    return "Correct the declared default; retrying unchanged will not help.";
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

  /** Write boundary (400); call only at the service boundary. */
  public UnsupportedClientOperationException toUnsupportedClient() {
    UnsupportedClientOperationException thrown =
        new UnsupportedClientOperationException(clientOperation(), getMessage());
    thrown.initCause(this);
    return thrown;
  }

  /** Read boundary (500): the stored defaults or ramp lookup, not the request, are at fault. */
  public IllegalStateException toServerError() {
    return new IllegalStateException(getMessage(), this);
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
