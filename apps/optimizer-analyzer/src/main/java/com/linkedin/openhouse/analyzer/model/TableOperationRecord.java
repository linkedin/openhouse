package com.linkedin.openhouse.analyzer.model;

import java.time.Instant;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Lightweight representation of an active table operation record. Mirrors the fields in {@link
 * com.linkedin.openhouse.optimizer.entity.TableOperationRow} that the Analyzer needs.
 */
@Data
@NoArgsConstructor
public class TableOperationRecord {

  private String id;
  private String tableUuid;
  private String databaseName;
  private String tableName;
  private String operationType;
  private String status;
  private Instant createdAt;
  private Instant scheduledAt;
}
