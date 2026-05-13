package com.linkedin.openhouse.analyzer.model;

import com.linkedin.openhouse.optimizer.entity.TableOperationRow;
import java.time.Instant;
import java.util.Comparator;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * An operation the analyzer has decided to schedule for a table. Built either from an existing
 * {@link TableOperationRow} (when loading current state) or from a {@link Table} (when creating a
 * new PENDING operation). Converts back to a JPA row via {@link #toRow()}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableOperation {

  /** Unique operation ID (UUID). */
  private String id;

  /** The table this operation targets. */
  private String tableUuid;

  /** Database name. */
  private String databaseName;

  /** Table name. */
  private String tableName;

  /** Operation type. */
  private OperationType operationType;

  /** Current lifecycle status. */
  private OperationStatus status;

  /** When this operation record was created. */
  private Instant createdAt;

  /** When the scheduler last submitted a job for this operation. */
  private Instant scheduledAt;

  /** Build a {@code TableOperation} from an existing JPA row. */
  public static TableOperation from(TableOperationRow row) {
    return TableOperation.builder()
        .id(row.getId())
        .tableUuid(row.getTableUuid())
        .databaseName(row.getDatabaseName())
        .tableName(row.getTableName())
        .operationType(OperationType.valueOf(row.getOperationType()))
        .status(OperationStatus.valueOf(row.getStatus()))
        .createdAt(row.getCreatedAt())
        .scheduledAt(row.getScheduledAt())
        .build();
  }

  /** Create a new PENDING operation for the given table and operation type. */
  public static TableOperation pending(Table table, OperationType operationType) {
    return TableOperation.builder()
        .id(UUID.randomUUID().toString())
        .tableUuid(table.getTableUuid())
        .databaseName(table.getDatabaseName())
        .tableName(table.getTableId())
        .operationType(operationType)
        .status(OperationStatus.PENDING)
        .createdAt(Instant.now())
        .build();
  }

  /** Convert to a JPA entity for persistence. */
  public TableOperationRow toRow() {
    return TableOperationRow.builder()
        .id(id)
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .operationType(operationType.name())
        .status(status.name())
        .createdAt(createdAt)
        .scheduledAt(scheduledAt)
        .version(0L)
        .build();
  }

  /** Return the more recently created of two operations. */
  public static TableOperation mostRecent(TableOperation a, TableOperation b) {
    Comparator<TableOperation> byCreatedAt =
        Comparator.comparing(r -> r.getCreatedAt() != null ? r.getCreatedAt() : Instant.EPOCH);
    return byCreatedAt.compare(a, b) >= 0 ? a : b;
  }
}
