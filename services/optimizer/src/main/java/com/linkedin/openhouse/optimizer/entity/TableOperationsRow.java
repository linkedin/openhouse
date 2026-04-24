package com.linkedin.openhouse.optimizer.entity;

import com.linkedin.openhouse.optimizer.api.model.OperationStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
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
 * state (SUCCESS/FAILED). Old terminal rows accumulate — they serve as implicit history. {@code
 * table_uuid} is the stable identity for the table (survives renames; rotates on drop+recreate).
 * The application enforces one active (PENDING or SCHEDULED) row per {@code (table_uuid,
 * operation_type)} at a time.
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

  @Column(name = "database_name", nullable = false, length = 255)
  private String databaseName;

  @Column(name = "table_name", nullable = false, length = 255)
  private String tableName;

  @Enumerated(EnumType.STRING)
  @Column(name = "operation_type", nullable = false, length = 50)
  private OperationType operationType;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false, length = 20)
  private OperationStatus status;

  /** When the Analyzer first created this row. Set by the service on insert; never updated. */
  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  /** Set when the operation is claimed; {@code null} while {@code PENDING}. */
  @Column(name = "scheduled_at")
  private Instant scheduledAt;

  /** Job ID returned by the Jobs Service after successful submission. */
  @Column(name = "job_id", length = 255)
  private String jobId;

  /**
   * Manual optimistic lock for the Scheduler claim. Incremented by the raw {@code claimOperation}
   * UPDATE query; must NOT use JPA {@code @Version} since the claim bypasses JPA entity management.
   */
  @Column(name = "version")
  private Long version;

  /**
   * Reserved for future per-operation metadata. Stored as JSON text; currently unused. The Analyzer
   * reads stats directly from {@code table_stats} instead of duplicating them here.
   */
  @Column(name = "metrics")
  private String metrics;
}
