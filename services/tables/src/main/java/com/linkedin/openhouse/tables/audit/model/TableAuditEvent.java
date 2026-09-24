package com.linkedin.openhouse.tables.audit.model;

import com.linkedin.openhouse.common.audit.model.BaseAuditEvent;
import com.linkedin.openhouse.internal.catalog.model.SnapshotRefChange;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Data Model for auditing each table operation. */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class TableAuditEvent extends BaseAuditEvent {
  private Instant eventTimestamp;

  private String clusterName;

  private String databaseName;

  private String tableName;

  private String user;

  private OperationType operationType;

  private OperationStatus operationStatus;

  private String currentTableRoot;

  private String grantor;

  private String grantee;

  private String role;

  private Long currentSnapshotId;

  private Long currentSnapshotTimestampMs;

  /** Ordered ref transitions from a successfully published metadata transaction. */
  private List<SnapshotRefChange> refChanges;

  /** Allowlisted subset of table properties at commit time, not the full property map. */
  private Map<String, String> auditedTableProperties;
}
