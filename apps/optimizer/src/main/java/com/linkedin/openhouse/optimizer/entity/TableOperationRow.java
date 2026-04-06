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
import lombok.Setter;

/** JPA entity mapping to the {@code table_operations} table in the optimizer DB. */
@Entity
@Table(name = "table_operations")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableOperationRow {

  @Id
  @Column(name = "id", nullable = false, length = 36)
  private String id;

  @Column(name = "table_uuid", nullable = false, length = 36)
  private String tableUuid;

  @Column(name = "database_name", nullable = false, length = 255)
  private String databaseName;

  @Column(name = "table_name", nullable = false, length = 255)
  private String tableName;

  @Column(name = "operation_type", nullable = false, length = 50)
  private String operationType;

  @Column(name = "status", nullable = false, length = 20)
  private String status;

  @Column(name = "created_at")
  private Instant createdAt;

  @Column(name = "scheduled_at")
  private Instant scheduledAt;

  @Column(name = "job_id", length = 255)
  private String jobId;

  /** Plain version column — not managed by JPA optimistic locking. */
  @Column(name = "version")
  private Long version;
}
