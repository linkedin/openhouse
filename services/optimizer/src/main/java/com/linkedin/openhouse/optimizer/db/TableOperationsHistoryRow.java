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
 * Append-only record of a completed maintenance operation.
 *
 * <p>Written when the operation-complete endpoint is called. The {@code id} is the same UUID as the
 * originating live-operations row, tying each history entry back to the operation cycle that
 * produced it. Multiple runs of the same operation on the same table produce multiple rows.
 *
 * <p>Self-contained DB-layer type: enums are {@link OperationType} / {@link HistoryStatus} from the
 * same package, JPA-bound as strings.
 */
@Entity
@Table(
    name = "table_operations_history",
    indexes = {
      @Index(name = "idx_table_uuid_hist", columnList = "table_uuid"),
      @Index(name = "idx_op_type_hist", columnList = "operation_type"),
      @Index(name = "idx_completed_at", columnList = "completed_at"),
      @Index(name = "idx_status_hist", columnList = "status"),
      @Index(name = "idx_toph_db_table", columnList = "database_name, table_name")
    })
@Getter
@EqualsAndHashCode
@Builder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class TableOperationsHistoryRow {

  /** Same UUID as the originating live-operations row. Set by the caller; not generated. */
  @Id
  @Column(name = "id", nullable = false, length = 36)
  private String id;

  /** Stable table identity from the Tables Service. */
  @Column(name = "table_uuid", nullable = false, length = 36)
  private String tableUuid;

  /** Denormalized database name. */
  @Column(name = "database_name", nullable = false, length = 128)
  private String databaseName;

  /** Denormalized table name. */
  @Column(name = "table_name", nullable = false, length = 128)
  private String tableName;

  /** The type of maintenance operation this history row records. */
  @Enumerated(EnumType.STRING)
  @Column(name = "operation_type", nullable = false, length = 50)
  private OperationType operationType;

  /** When the operation completed, as recorded by the complete endpoint. */
  @Column(name = "completed_at", nullable = false)
  private Instant completedAt;

  /** Terminal outcome: {@link HistoryStatus#SUCCESS} or {@link HistoryStatus#FAILED}. */
  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false, length = 20)
  private HistoryStatus status;
}
