package com.linkedin.openhouse.optimizer.api.spec;

import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** DTO for {@code table_operations_history} — append-only operation results. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableOperationsHistory {

  /** Same UUID as the originating {@code table_operations.id}; supplied by the caller. */
  private String id;

  /** Stable table identity from the Tables Service. */
  private String tableUuid;

  /** Denormalized database name for display. */
  private String databaseName;

  /** Denormalized table name for display. */
  private String tableName;

  /** The type of maintenance operation this history row records. */
  private OperationType operationType;

  /** When the operation completed, as recorded by the complete endpoint. */
  private Instant completedAt;

  /** {@code SUCCESS} or {@code FAILED}. */
  private HistoryStatus status;

  /** OFD-specific: number of orphan files deleted; null if not OFD or on failure. */
  private Long orphanFilesDeleted;

  /** OFD-specific: bytes reclaimed by orphan file deletion; null if not OFD or on failure. */
  private Long orphanBytesDeleted;

  /** On failure, the message from the Spark-side exception. Null on success. */
  private String errorMessage;

  /** On failure, the simple name of the Spark-side exception class. Null on success. */
  private String errorType;

  /** Convert to the internal-model counterpart. */
  public TableOperationsHistoryDto toModel() {
    return TableOperationsHistoryDto.builder()
        .id(id)
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .operationType(operationType == null ? null : operationType.toModel())
        .completedAt(completedAt)
        .status(status == null ? null : status.toModel())
        .orphanFilesDeleted(orphanFilesDeleted)
        .orphanBytesDeleted(orphanBytesDeleted)
        .errorMessage(errorMessage)
        .errorType(errorType)
        .build();
  }

  /** Build a wire DTO from the internal-model counterpart. */
  public static TableOperationsHistory fromModel(TableOperationsHistoryDto h) {
    if (h == null) {
      return null;
    }
    return TableOperationsHistory.builder()
        .id(h.getId())
        .tableUuid(h.getTableUuid())
        .databaseName(h.getDatabaseName())
        .tableName(h.getTableName())
        .operationType(OperationType.fromModel(h.getOperationType()))
        .completedAt(h.getCompletedAt())
        .status(HistoryStatus.fromModel(h.getStatus()))
        .orphanFilesDeleted(h.getOrphanFilesDeleted())
        .orphanBytesDeleted(h.getOrphanBytesDeleted())
        .errorMessage(h.getErrorMessage())
        .errorType(h.getErrorType())
        .build();
  }
}
