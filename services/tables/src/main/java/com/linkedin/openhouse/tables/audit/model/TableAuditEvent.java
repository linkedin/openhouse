package com.linkedin.openhouse.tables.audit.model;

import com.linkedin.openhouse.common.audit.model.BaseAuditEvent;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Data Model for auditing each table operation. */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
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
}
