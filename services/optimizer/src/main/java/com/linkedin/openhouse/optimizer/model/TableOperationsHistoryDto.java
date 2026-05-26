package com.linkedin.openhouse.optimizer.model;

import com.linkedin.openhouse.optimizer.db.TableOperationsHistoryRow;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Internal-model view of a completed operation history record.
 *
 * <p>Mirrors the field set of the underlying history row but in internal types only. Used by
 * components that need to reason about completed operations (e.g., scheduling-cadence analyzers).
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class TableOperationsHistoryDto {

  /** Same UUID as the originating live-operations row. */
  private String id;

  /** Stable table identity from the Tables Service. */
  private String tableUuid;

  /** Denormalized database name. */
  private String databaseName;

  /** Denormalized table name. */
  private String tableName;

  /** Operation type for this completed run. */
  private OperationTypeDto operationType;

  /** When the operation completed, as recorded by the complete endpoint. */
  private Instant completedAt;

  /** Terminal outcome: {@link HistoryStatusDto#SUCCESS} or {@link HistoryStatusDto#FAILED}. */
  private HistoryStatusDto status;

  /** OFD-specific: number of orphan files deleted; null if not an OFD operation or on failure. */
  private Long orphanFilesDeleted;

  /** OFD-specific: bytes reclaimed by orphan file deletion; null if not OFD or on failure. */
  private Long orphanBytesDeleted;

  /** On failure, the message from the Spark-side exception. Null on success. */
  private String errorMessage;

  /** On failure, the simple name of the Spark-side exception class. Null on success. */
  private String errorType;

  /** Convert to the corresponding DB row. */
  public TableOperationsHistoryRow toRow() {
    return TableOperationsHistoryRow.builder()
        .id(id)
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .operationType(operationType == null ? null : operationType.toDb())
        .completedAt(completedAt)
        .status(status == null ? null : status.toDb())
        .orphanFilesDeleted(orphanFilesDeleted)
        .orphanBytesDeleted(orphanBytesDeleted)
        .errorMessage(errorMessage)
        .errorType(errorType)
        .build();
  }

  /** Build a {@link TableOperationsHistoryDto} from a DB row. */
  public static TableOperationsHistoryDto fromRow(TableOperationsHistoryRow row) {
    if (row == null) {
      return null;
    }
    return TableOperationsHistoryDto.builder()
        .id(row.getId())
        .tableUuid(row.getTableUuid())
        .databaseName(row.getDatabaseName())
        .tableName(row.getTableName())
        .operationType(OperationTypeDto.fromDb(row.getOperationType()))
        .completedAt(row.getCompletedAt())
        .status(HistoryStatusDto.fromDb(row.getStatus()))
        .orphanFilesDeleted(row.getOrphanFilesDeleted())
        .orphanBytesDeleted(row.getOrphanBytesDeleted())
        .errorMessage(row.getErrorMessage())
        .errorType(row.getErrorType())
        .build();
  }

  /**
   * Return whichever of {@code this} and {@code other} completed later (or {@code this} on tie).
   * Shaped for use as a {@link java.util.function.BinaryOperator} in stream collectors.
   */
  public TableOperationsHistoryDto after(TableOperationsHistoryDto other) {
    return this.completedAt.isBefore(other.completedAt) ? other : this;
  }
}
