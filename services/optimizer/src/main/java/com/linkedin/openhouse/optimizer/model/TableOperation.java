package com.linkedin.openhouse.optimizer.model;

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
 * <p>Pure internal-model type — no references to wire-API or DB types. Cross-layer construction
 * happens via {@link com.linkedin.openhouse.optimizer.model.mapper.ModelDbMapper} (DB boundary) or
 * {@link com.linkedin.openhouse.optimizer.model.mapper.ApiModelMapper} (API boundary).
 *
 * <p>{@link #fileCount} is a non-persisted enrichment populated by consumers that need it (e.g.,
 * the OFD scheduler reads it from {@code table_stats} for bin-packing). The DB column does not
 * carry it.
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

  /**
   * Number of current data files on the table at evaluation time. Non-persisted enrichment;
   * populated by consumers that need it. Null when not enriched.
   */
  private Long fileCount;

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
}
