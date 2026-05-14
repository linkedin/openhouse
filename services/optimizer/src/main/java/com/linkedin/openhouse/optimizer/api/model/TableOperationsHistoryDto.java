package com.linkedin.openhouse.optimizer.api.model;

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
  private OperationType operationType;

  /** When the operation completed, as recorded by the complete endpoint. */
  private Instant completedAt;

  /** {@code SUCCESS} or {@code FAILED}. */
  private HistoryStatus status;
}
