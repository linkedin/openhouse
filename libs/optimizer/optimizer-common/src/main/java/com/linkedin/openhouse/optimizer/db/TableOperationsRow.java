package com.linkedin.openhouse.optimizer.db;

import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * JPA entity representing an Analyzer recommendation for a table maintenance operation.
 *
 * <p>Each row is identified by a client-generated UUID ({@code id}). The Analyzer creates a new row
 * when it first recommends an operation for a table, or when re-recommending after a prior terminal
 * state. {@code table_uuid} is the stable identity for the table (survives renames; rotates on
 * drop+recreate). The application enforces one active (PENDING / SCHEDULING / SCHEDULED) row per
 * {@code (table_uuid, operation_type)} at a time.
 */
@Entity
@Table(
    name = "table_operations",
    indexes = {
      @Index(name = "idx_to_table_uuid_optype", columnList = "table_uuid, operation_type"),
      @Index(name = "idx_to_optype_status", columnList = "operation_type, status")
    })
@Getter
@EqualsAndHashCode
@Builder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class TableOperationsRow {

  /** Client-generated UUID identifying this specific operation recommendation. */
  @Id
  @Column(name = "id", nullable = false, length = 36)
  private String id;

  /** Stable table identity from the Tables Service. Survives renames; rotates on drop+recreate. */
  @Column(name = "table_uuid", nullable = false, length = 36)
  private String tableUuid;

  /** Denormalized database name. */
  @Column(name = "database_name", nullable = false, length = 128)
  private String databaseName;

  /** Denormalized table name. */
  @Column(name = "table_name", nullable = false, length = 128)
  private String tableName;

  /** The type of maintenance operation this row recommends. */
  @Enumerated(EnumType.STRING)
  @Column(name = "operation_type", nullable = false, length = 50)
  private OperationType operationType;

  /** Lifecycle state — drives the scheduler's CAS claim and the analyzer's eligibility check. */
  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false, length = 20)
  private OperationStatus status;

  /** When the analyzer first created this row. Set on insert; never updated. */
  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  /** When the scheduler last submitted a job for this row. {@code null} while {@code PENDING}. */
  @Column(name = "scheduled_at")
  private Instant scheduledAt;

  /** Spark job ID written by the scheduler at claim time. Internal-only; never exposed on wire. */
  @Column(name = "job_id", length = 255)
  private String jobId;

  /** Convert this persistence row to the Spring-free optimizer model. */
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
        .jobId(jobId)
        .build();
  }

  /** Build a persistence row from the Spring-free optimizer model. */
  public static TableOperationsRow fromModel(TableOperationDto operation) {
    if (operation == null) {
      return null;
    }
    return TableOperationsRow.builder()
        .id(operation.getId())
        .tableUuid(operation.getTableUuid())
        .databaseName(operation.getDatabaseName())
        .tableName(operation.getTableName())
        .operationType(OperationType.fromModel(operation.getOperationType()))
        .status(OperationStatus.fromModel(operation.getStatus()))
        .createdAt(operation.getCreatedAt())
        .scheduledAt(operation.getScheduledAt())
        .jobId(operation.getJobId())
        .build();
  }
}
