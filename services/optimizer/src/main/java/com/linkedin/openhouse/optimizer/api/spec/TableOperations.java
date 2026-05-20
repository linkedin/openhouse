package com.linkedin.openhouse.optimizer.api.spec;

import com.linkedin.openhouse.optimizer.model.TableOperationDto;
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
public class TableOperations {

  /** Client-generated UUID identifying this specific operation recommendation. */
  private String id;

  /** Stable table identity from the Tables Service. */
  private String tableUuid;

  /** Denormalized database name for display; not part of the primary key. */
  private String databaseName;

  /** Denormalized table name for display; not part of the primary key. */
  private String tableName;

  /** The type of maintenance operation (e.g. ORPHAN_FILES_DELETION). */
  private OperationType operationType;

  /** {@code PENDING} or {@code SCHEDULED}. Defaults to {@code PENDING} on creation. */
  private OperationStatus status;

  /** Server-set when the row is first created by the Analyzer. */
  private Instant createdAt;

  /** Set by the Scheduler when claiming; {@code null} while PENDING. */
  private Instant scheduledAt;

  /** Job ID returned by the Jobs Service after successful submission. */
  private String jobId;

  /** Convert to the internal-model counterpart. */
  public TableOperationDto toModel() {
    return TableOperationDto.builder()
        .id(id)
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .operationType(operationType == null ? null : operationType.toModel())
        .status(status == null ? null : status.toModel())
        .createdAt(createdAt)
        .scheduledAt(scheduledAt)
        .build();
  }

  /** Build a wire DTO from the internal-model counterpart. */
  public static TableOperations fromModel(TableOperationDto op) {
    if (op == null) {
      return null;
    }
    return TableOperations.builder()
        .id(op.getId())
        .tableUuid(op.getTableUuid())
        .databaseName(op.getDatabaseName())
        .tableName(op.getTableName())
        .operationType(OperationType.fromModel(op.getOperationType()))
        .status(OperationStatus.fromModel(op.getStatus()))
        .createdAt(op.getCreatedAt())
        .scheduledAt(op.getScheduledAt())
        .build();
  }
}
