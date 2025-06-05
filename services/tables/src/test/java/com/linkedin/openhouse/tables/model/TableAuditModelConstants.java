package com.linkedin.openhouse.tables.model;

import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.OperationType;
import com.linkedin.openhouse.tables.audit.model.TableAuditEvent;

public final class TableAuditModelConstants {
  private static final String CLUSTER_NAME = "local-cluster";
  private static final String USER = "testUser";
  private static final String MOCK_USER = "DUMMY_ANONYMOUS_USER";

  public static final String[] EXCLUDE_FIELDS = new String[] {"eventTimestamp", "currentTableRoot"};

  public static final TableAuditEvent TABLE_AUDIT_EVENT_CREATE_TABLE_SUCCESS_E2E =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d1")
          .tableName("t1")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.CREATE)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_GET_TABLE_SUCCESS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d200")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.READ)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_GET_TABLE_FAILED =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d404")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.FAILED)
          .operationType(OperationType.READ)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_GET_ALL_TABLES_SUCCESS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d200")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.READ)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_GET_ALL_TABLES_FAILED =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d404")
          .user(USER)
          .operationStatus(OperationStatus.FAILED)
          .operationType(OperationType.READ)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_CREATE_TABLE_SUCCESS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d200")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.CREATE)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_CREATE_TABLE_FAILED =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d409")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.FAILED)
          .operationType(OperationType.CREATE)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_STAGED_CREATE_TABLE_SUCCESS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d200")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.STAGED_CREATE)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_STAGED_CREATE_TABLE_FAILED =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d404")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.FAILED)
          .operationType(OperationType.STAGED_CREATE)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_UPDATE_TABLE_SUCCESS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d200")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.UPDATE)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_UPDATE_TABLE_FAILED =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d404")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.FAILED)
          .operationType(OperationType.UPDATE)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_DELETE_TABLE_SUCCESS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d200")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.DELETE)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_DELETE_TABLE_FAILED =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d400")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.FAILED)
          .operationType(OperationType.DELETE)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_RENAME_TABLE_SUCCESS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d1")
          .tableName("t1")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.RENAME)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_RENAME_TABLE_FAILED =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d1")
          .tableName("t1")
          .user(USER)
          .operationStatus(OperationStatus.FAILED)
          .operationType(OperationType.RENAME)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_GET_ACL_POLICIES_SUCCESS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d200")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.READ)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_GET_ACL_POLICIES_FAILED =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d400")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.FAILED)
          .operationType(OperationType.READ)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_UPDATE_ACL_POLICIES_SUCCESS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d204")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.GRANT)
          .grantor(USER)
          .grantee(MOCK_USER)
          .role("AclEditor")
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_UPDATE_ACL_POLICIES_FAILED =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d400")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.FAILED)
          .operationType(OperationType.GRANT)
          .grantor(USER)
          .grantee(MOCK_USER)
          .role("AclEditor")
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_PUT_ICEBERG_SNAPSHOTS_SUCCESS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d200")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.COMMIT)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_PUT_ICEBERG_SNAPSHOTS_FAILED =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d400")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.FAILED)
          .operationType(OperationType.COMMIT)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_PUT_ICEBERG_SNAPSHOTS_CTAS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d200")
          .tableName("tb1")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.STAGED_COMMIT)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_GET_ALL_DATABASES_SUCCESS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.READ)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_GET_DATABASES_ACL_POLICIES_SUCCESS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d200")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.READ)
          .build();

  public static final TableAuditEvent TABLE_AUDIT_EVENT_UPDATE_DATABASES_ACL_POLICIES_SUCCESS =
      TableAuditEvent.builder()
          .clusterName(CLUSTER_NAME)
          .databaseName("d200")
          .user(USER)
          .operationStatus(OperationStatus.SUCCESS)
          .operationType(OperationType.GRANT)
          .grantor(USER)
          .grantee(MOCK_USER)
          .role("AclEditor")
          .build();

  private TableAuditModelConstants() {}
}
