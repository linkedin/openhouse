package com.linkedin.openhouse.optimizer.api.model;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** DTO for {@code table_operations} — Analyzer recommendations read by the Scheduler. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableOperationsDto {

  /** Client-generated UUID identifying this specific operation recommendation. */
  private String id;

  /** Stable table identity from the Tables Service. */
  private String tableUuid;

  private String databaseName;
  private String tableName;
  private OperationType operationType;

  /** {@code PENDING} or {@code SCHEDULED}. Defaults to {@code PENDING} on creation. */
  private OperationStatus status;

  /** Server-set when the row is first created by the Analyzer. */
  private Instant createdAt;

  /** Set by the Scheduler when claiming; {@code null} while PENDING. */
  private Instant scheduledAt;

  /** Job ID returned by the Jobs Service after successful submission. */
  private String jobId;

  /** Denormalized stats snapshot captured at analysis time. */
  private OperationMetrics metrics;
}
