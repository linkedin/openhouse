package com.linkedin.openhouse.tables.readbridge;

import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Objects;
import lombok.Getter;

/**
 * Checked column-default validation failures. Carries domain context, never HTTP status or raw
 * diagnostics for a client. The operation boundary identifies stored versus incoming metadata; the
 * HTTP handler owns public messages and status mapping.
 */
@Getter
public final class ColumnDefaultException extends Exception {
  public enum Reason {
    INVALID_SCHEMA,
    INVALID_VALUE,
    TYPE_MISMATCH,
    OUT_OF_RANGE,
    REMOVED,
    REWRITE,
    UNAVAILABLE,
    INTERNAL
  }

  public enum Origin {
    UNKNOWN,
    STORED,
    INCOMING
  }

  private final Reason reason;
  private final Origin origin;
  private final String databaseId;
  private final String tableId;
  private final Integer fieldId;
  private final String columnPath;
  private final String sourceType;
  private final String targetType;

  public ColumnDefaultException(Reason reason, TableDto table, Throwable cause) {
    this(reason, table, null, null, null, null, cause);
  }

  public ColumnDefaultException(
      Reason reason,
      TableDto table,
      Integer fieldId,
      String columnPath,
      String sourceType,
      String targetType,
      Throwable cause) {
    this(
        reason,
        Origin.UNKNOWN,
        table == null ? null : table.getDatabaseId(),
        table == null ? null : table.getTableId(),
        fieldId,
        columnPath,
        sourceType,
        targetType,
        cause);
  }

  private ColumnDefaultException(
      Reason reason,
      Origin origin,
      String databaseId,
      String tableId,
      Integer fieldId,
      String columnPath,
      String sourceType,
      String targetType,
      Throwable cause) {
    super(String.format("Column defaults %s for %s.%s", reason, databaseId, tableId), cause);
    this.reason = Objects.requireNonNull(reason, "reason");
    this.origin = Objects.requireNonNull(origin, "origin");
    this.databaseId = databaseId;
    this.tableId = tableId;
    this.fieldId = fieldId;
    this.columnPath = columnPath;
    this.sourceType = sourceType;
    this.targetType = targetType;
  }

  /** Attaches operation context without replacing an already identified metadata origin. */
  public ColumnDefaultException withOrigin(Origin metadataOrigin) {
    if (origin != Origin.UNKNOWN || metadataOrigin == Origin.UNKNOWN) {
      return this;
    }
    return new ColumnDefaultException(
        reason,
        metadataOrigin,
        databaseId,
        tableId,
        fieldId,
        columnPath,
        sourceType,
        targetType,
        this);
  }
}
