package com.linkedin.openhouse.optimizer.entity;

import com.linkedin.openhouse.optimizer.api.model.JobResult;
import com.linkedin.openhouse.optimizer.api.model.OperationHistoryStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
import com.linkedin.openhouse.optimizer.config.JobResultConverter;
import java.time.Instant;
import javax.persistence.Column;
import javax.persistence.Convert;
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
 * Append-only record of a completed Spark maintenance job.
 *
 * <p>Written by the Spark app after each table's operation finishes. The {@code id} is the same
 * UUID as the originating {@code table_operations.id}, tying each history entry directly back to
 * the specific operation cycle that produced it. Multiple runs of the same operation on the same
 * table produce multiple rows (each cycle gets a new UUID from the Analyzer).
 */
@Entity
@Table(
    name = "table_operations_history",
    indexes = {
      @Index(name = "idx_table_uuid_hist", columnList = "table_uuid"),
      @Index(name = "idx_op_type_hist", columnList = "operation_type"),
      @Index(name = "idx_submitted_at", columnList = "submitted_at"),
      @Index(name = "idx_status_hist", columnList = "status"),
      @Index(name = "idx_job_id", columnList = "job_id")
    })
@Getter
@EqualsAndHashCode
@Builder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class TableOperationsHistoryRow {

  /** Same UUID as the originating {@code table_operations.id}. Set by the caller; not generated. */
  @Id
  @Column(name = "id", nullable = false, length = 36)
  private String id;

  @Column(name = "table_uuid", nullable = false, length = 36)
  private String tableUuid;

  @Column(name = "database_name", nullable = false, length = 255)
  private String databaseName;

  @Column(name = "table_name", nullable = false, length = 255)
  private String tableName;

  @Enumerated(EnumType.STRING)
  @Column(name = "operation_type", nullable = false, length = 50)
  private OperationType operationType;

  /** When the Spark job was submitted / ran, as reported by the job itself. */
  @Column(name = "submitted_at", nullable = false)
  private Instant submittedAt;

  /** {@code SUCCESS} or {@code FAILED}. */
  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false, length = 20)
  private OperationHistoryStatus status;

  /** Spark job ID; indexed for job → result lookups. */
  @Column(name = "job_id", length = 255)
  private String jobId;

  /** Job result: error details on failure, both fields null on success. */
  @Convert(converter = JobResultConverter.class)
  @Column(name = "result")
  private JobResult result;

  /** Number of orphan files deleted by the Spark job; null for non-OFD operations. */
  @Column(name = "orphan_files_deleted")
  private Integer orphanFilesDeleted;

  /** Bytes reclaimed by orphan file deletion; null for non-OFD operations. */
  @Column(name = "orphan_bytes_deleted")
  private Long orphanBytesDeleted;
}
