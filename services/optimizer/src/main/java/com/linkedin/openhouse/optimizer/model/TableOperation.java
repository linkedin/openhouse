package com.linkedin.openhouse.optimizer.model;

import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import java.time.Instant;
import java.util.Comparator;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * An operation the analyzer has decided to schedule for a table, and that the scheduler later picks
 * up and submits.
 *
 * <p>Conversion methods cross into the DB layer one-way; the inverse lives on the api side. db/
 * types know nothing about model/ or api/.
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

  /** Return the more recently created of two operations. */
  public static TableOperation mostRecent(TableOperation a, TableOperation b) {
    Comparator<TableOperation> byCreatedAt =
        Comparator.comparing(r -> r.getCreatedAt() != null ? r.getCreatedAt() : Instant.EPOCH);
    return byCreatedAt.compare(a, b) >= 0 ? a : b;
  }

  /** Convert to the corresponding DB row. */
  public TableOperationsRow toRow() {
    return TableOperationsRow.builder()
        .id(id)
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .operationType(operationType == null ? null : operationType.toDb())
        .status(status == null ? null : status.toDb())
        .createdAt(createdAt)
        .scheduledAt(scheduledAt)
        .build();
  }

  /** Build a {@link TableOperation} from a DB row. */
  public static TableOperation fromRow(TableOperationsRow row) {
    if (row == null) {
      return null;
    }
    return TableOperation.builder()
        .id(row.getId())
        .tableUuid(row.getTableUuid())
        .databaseName(row.getDatabaseName())
        .tableName(row.getTableName())
        .operationType(OperationType.fromDb(row.getOperationType()))
        .status(OperationStatus.fromDb(row.getStatus()))
        .createdAt(row.getCreatedAt())
        .scheduledAt(row.getScheduledAt())
        .build();
  }
}
