package com.linkedin.openhouse.analyzer.model;

import com.linkedin.openhouse.optimizer.entity.TableOperationRow;
import java.time.Instant;
import java.util.Comparator;
import java.util.UUID;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * An operation the analyzer has decided to schedule for a table. Built either from an existing
 * {@link TableOperationRow} (when loading current state) or from a {@link Table} (when creating a
 * new PENDING operation). Converts back to a JPA row via {@link #toRow()}.
 */
@Data
@NoArgsConstructor
public class TableOperation {

  /** Unique operation ID (UUID). */
  private String id;

  /** The table this operation targets. */
  private String tableUuid;

  /** Database name, denormalized for display. */
  private String databaseName;

  /** Table name, denormalized for display. */
  private String tableName;

  /** Operation type (e.g., {@code "ORPHAN_FILES_DELETION"}). */
  private String operationType;

  /** Current lifecycle status: PENDING, SCHEDULING, SCHEDULED. */
  private String status;

  /** When this operation record was created. */
  private Instant createdAt;

  /** When the scheduler last submitted a job for this operation. */
  private Instant scheduledAt;

  /** Build a {@code TableOperation} from an existing JPA row. */
  public static TableOperation from(TableOperationRow row) {
    TableOperation op = new TableOperation();
    op.id = row.getId();
    op.tableUuid = row.getTableUuid();
    op.databaseName = row.getDatabaseName();
    op.tableName = row.getTableName();
    op.operationType = row.getOperationType();
    op.status = row.getStatus();
    op.createdAt = row.getCreatedAt();
    op.scheduledAt = row.getScheduledAt();
    return op;
  }

  /** Create a new PENDING operation for the given table and operation type. */
  public static TableOperation pending(Table table, String operationType) {
    TableOperation op = new TableOperation();
    op.id = UUID.randomUUID().toString();
    op.tableUuid = table.getTableUuid();
    op.databaseName = table.getDatabaseId();
    op.tableName = table.getTableId();
    op.operationType = operationType;
    op.status = "PENDING";
    op.createdAt = Instant.now();
    return op;
  }

  /** Convert to a JPA entity for persistence. */
  public TableOperationRow toRow() {
    return TableOperationRow.builder()
        .id(id)
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .operationType(operationType)
        .status(status)
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
