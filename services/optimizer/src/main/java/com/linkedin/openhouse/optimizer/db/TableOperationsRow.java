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

  @Id
  @Column(name = "id", nullable = false, length = 36)
  private String id;

  @Column(name = "table_uuid", nullable = false, length = 36)
  private String tableUuid;

  @Column(name = "database_name", nullable = false, length = 128)
  private String databaseName;

  @Column(name = "table_name", nullable = false, length = 128)
  private String tableName;

  @Enumerated(EnumType.STRING)
  @Column(name = "operation_type", nullable = false, length = 50)
  private OperationType operationType;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false, length = 20)
  private OperationStatus status;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Column(name = "scheduled_at")
  private Instant scheduledAt;

  /** Spark job ID written by the scheduler at claim time. Internal-only; never exposed on wire. */
  @Column(name = "job_id", length = 255)
  private String jobId;

  /**
   * Monotonically-increasing version for application-level optimistic concurrency control. The
   * scheduler's batch CAS transitions match this in the WHERE clause and bump it by one on UPDATE,
   * ensuring two scheduler instances can't both move the same row out of PENDING. Not managed by
   * JPA optimistic locking — kept as a plain column so the WHERE-clause-based CAS pattern works
   * portably across MySQL and H2.
   */
  @Column(name = "version")
  private Long version;
}
