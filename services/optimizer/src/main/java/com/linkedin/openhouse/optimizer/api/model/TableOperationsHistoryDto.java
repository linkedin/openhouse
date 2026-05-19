package com.linkedin.openhouse.optimizer.api.model;

import com.linkedin.openhouse.optimizer.model.TableOperationsHistory;
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
public class TableOperationsHistoryDto {

  /** Same UUID as the originating {@code table_operations.id}; supplied by the caller. */
  private String id;

  /** Stable table identity from the Tables Service. */
  private String tableUuid;

  /** Denormalized database name for display. */
  private String databaseName;

  /** Denormalized table name for display. */
  private String tableName;

  /** The type of maintenance operation this history row records. */
  private OperationTypeDto operationType;

  /** When the operation completed, as recorded by the complete endpoint. */
  private Instant completedAt;

  /** {@code SUCCESS} or {@code FAILED}. */
  private HistoryStatusDto status;

  /** Convert to the internal-model counterpart. */
  public TableOperationsHistory toModel() {
    return TableOperationsHistory.builder()
        .id(id)
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .operationType(operationType == null ? null : operationType.toModel())
        .completedAt(completedAt)
        .status(status == null ? null : status.toModel())
        .build();
  }

  /** Build a wire DTO from the internal-model counterpart. */
  public static TableOperationsHistoryDto fromModel(TableOperationsHistory h) {
    if (h == null) {
      return null;
    }
    return TableOperationsHistoryDto.builder()
        .id(h.getId())
        .tableUuid(h.getTableUuid())
        .databaseName(h.getDatabaseName())
        .tableName(h.getTableName())
        .operationType(OperationTypeDto.fromModel(h.getOperationType()))
        .completedAt(h.getCompletedAt())
        .status(HistoryStatusDto.fromModel(h.getStatus()))
        .build();
  }
}
