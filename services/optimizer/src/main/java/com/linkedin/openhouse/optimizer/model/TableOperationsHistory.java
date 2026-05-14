package com.linkedin.openhouse.optimizer.model;

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
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableOperationsHistory {

  /** Same UUID as the originating live-operations row. */
  private String id;

  /** Stable table identity from the Tables Service. */
  private String tableUuid;

  /** Denormalized database name. */
  private String databaseName;

  /** Denormalized table name. */
  private String tableName;

  /** Operation type for this completed run. */
  private OperationType operationType;

  /** When the operation completed, as recorded by the complete endpoint. */
  private Instant completedAt;

  /** Terminal outcome: {@link HistoryStatus#SUCCESS} or {@link HistoryStatus#FAILED}. */
  private HistoryStatus status;
}
