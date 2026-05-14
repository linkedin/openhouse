package com.linkedin.openhouse.optimizer.db;

import java.time.Instant;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;
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
 *
 * <p>Self-contained DB-layer type: enums are {@link OperationType} / {@link OperationStatus} from
 * the same package, JPA-bound as strings.
 */
@Entity
@Table(
    name = "table_operations",
    indexes = {
      @Index(name = "idx_table_uuid", columnList = "table_uuid"),
      @Index(name = "idx_op_type", columnList = "operation_type"),
      @Index(name = "idx_status", columnList = "status"),
      @Index(name = "idx_created_at", columnList = "created_at"),
      @Index(name = "idx_scheduled_at", columnList = "scheduled_at")
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
}
