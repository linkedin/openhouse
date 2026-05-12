package com.linkedin.openhouse.optimizer.entity;

import java.time.Instant;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Lightweight JPA entity for reading {@code table_operations_history} rows. */
@Entity
@Table(name = "table_operations_history")
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableOperationHistoryRow {

  @Id
  @Column(name = "id", nullable = false, length = 36)
  private String id;

  @Column(name = "table_uuid", nullable = false, length = 36)
  private String tableUuid;

  @Column(name = "database_name", nullable = false, length = 128)
  private String databaseName;

  @Column(name = "table_name", nullable = false, length = 128)
  private String tableName;

  @Column(name = "operation_type", nullable = false, length = 50)
  private String operationType;

  @Column(name = "completed_at", nullable = false)
  private Instant completedAt;

  @Column(name = "status", nullable = false, length = 20)
  private String status;
}
