package com.linkedin.openhouse.tables.audit;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.common.security.AuthenticationUtils.extractAuthenticatedUserPrincipal;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.audit.AuditHandler;
import com.linkedin.openhouse.tables.api.handler.impl.OpenHouseTablesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.audit.model.OperationStatus;
import com.linkedin.openhouse.tables.audit.model.OperationType;
import com.linkedin.openhouse.tables.audit.model.TableAuditEvent;
import com.linkedin.openhouse.tables.config.InternalCatalogProperties;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefParser;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Aspect class to support table operation auditing for all controllers. It enhances the ability of
 * particular methods by adding logic of building and emitting audit events.
 */
@Slf4j
@Aspect
@Component
public class TableAuditAspect {

  @Autowired private ClusterProperties clusterProperties;

  @Autowired private AuditHandler<TableAuditEvent> tableAuditHandler;

  @Autowired private InternalCatalogProperties internalCatalogProperties;

  /**
   * Install the Around advice for getTable() method in OpenHouseTablesApiHandler.
   *
   * @param point The api handler method being enhanced
   * @return Result of the api handler method
   * @throws Throwable Any exception during execution of the api handler method
   */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody> "
          + "com.linkedin.openhouse.tables.api.handler.TablesApiHandler.getTable(..)) "
          + "&& args(databaseId, tableId, actingPrincipal)")
  protected ApiResponse<GetTableResponseBody> auditGetTable(
      ProceedingJoinPoint point, String databaseId, String tableId, String actingPrincipal)
      throws Throwable {
    ApiResponse<GetTableResponseBody> result = null;
    TableAuditEvent event =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .databaseName(databaseId)
            .tableName(tableId)
            .operationType(OperationType.READ)
            .build();
    try {
      result = (ApiResponse<GetTableResponseBody>) point.proceed();
      buildAndSendEvent(
          event, OperationStatus.SUCCESS, result.getResponseBody().getTableLocation());
    } catch (Throwable t) {
      // Table operation failed. Audit this failed event, and throw the error to be captured by
      // {@link com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler}
      buildAndSendEvent(event, OperationStatus.FAILED, null);
      throw t;
    }
    return result;
  }

  /** Install the Around advice for getAllTables() method in OpenHouseTablesApiHandler */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody> "
          + "com.linkedin.openhouse.tables.api.handler.TablesApiHandler.getAllTables(..)) "
          + "&& args(databaseId)")
  protected ApiResponse<GetAllTablesResponseBody> auditGetAllTables(
      ProceedingJoinPoint point, String databaseId) throws Throwable {
    ApiResponse<GetAllTablesResponseBody> result = null;
    TableAuditEvent event =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .databaseName(databaseId)
            .operationType(OperationType.READ)
            .build();
    try {
      result = (ApiResponse<GetAllTablesResponseBody>) point.proceed();
      buildAndSendEvent(event, OperationStatus.SUCCESS, null);
    } catch (Throwable t) {
      buildAndSendEvent(event, OperationStatus.FAILED, null);
      throw t;
    }
    return result;
  }

  /** Install the Around advice for createTable() method in OpenHouseTablesApiHandler */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody> "
          + "com.linkedin.openhouse.tables.api.handler.TablesApiHandler.createTable(..)) "
          + "&& args(databaseId, createUpdateTableRequestBody, tableCreator)")
  protected ApiResponse<GetTableResponseBody> auditCreateTable(
      ProceedingJoinPoint point,
      String databaseId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody,
      String tableCreator)
      throws Throwable {
    ApiResponse<GetTableResponseBody> result = null;
    OperationType operationType = null;
    if (createUpdateTableRequestBody.isStageCreate()) {
      operationType = OperationType.STAGED_CREATE;
    } else if (createUpdateTableRequestBody.isStageReplace()) {
      operationType = OperationType.STAGED_REPLACE;
    } else {
      operationType = OperationType.CREATE;
    }
    TableAuditEvent event =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .databaseName(databaseId)
            .tableName(createUpdateTableRequestBody.getTableId())
            .operationType(operationType)
            .build();
    try {
      result = (ApiResponse<GetTableResponseBody>) point.proceed();
      buildAndSendEvent(
          event, OperationStatus.SUCCESS, result.getResponseBody().getTableLocation());

    } catch (Throwable t) {
      buildAndSendEvent(event, OperationStatus.FAILED, null);
      throw t;
    }
    return result;
  }

  /** Install the Around advice for updateTable() method in OpenHouseTablesApiHandler */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody> "
          + "com.linkedin.openhouse.tables.api.handler.TablesApiHandler.updateTable(..)) "
          + "&& args(databaseId, tableId, createUpdateTableRequestBody, tableCreator)")
  protected ApiResponse<GetTableResponseBody> auditUpdateTable(
      ProceedingJoinPoint point,
      String databaseId,
      String tableId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody,
      String tableCreator)
      throws Throwable {
    ApiResponse<GetTableResponseBody> result = null;
    TableAuditEvent event =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .databaseName(databaseId)
            .tableName(tableId)
            .operationType(
                createUpdateTableRequestBody.getBaseTableVersion().equals(INITIAL_TABLE_VERSION)
                    ? OperationType.CREATE
                    : OperationType.UPDATE)
            .build();
    try {
      result = (ApiResponse<GetTableResponseBody>) point.proceed();
      buildAndSendEvent(
          event, OperationStatus.SUCCESS, result.getResponseBody().getTableLocation());
    } catch (Throwable t) {
      buildAndSendEvent(event, OperationStatus.FAILED, null);
      throw t;
    }
    return result;
  }

  /** Install the Around advice for deleteTable() method in OpenHouseTablesApiHandler */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<Void> "
          + "com.linkedin.openhouse.tables.api.handler.TablesApiHandler.deleteTable(..)) "
          + "&& args(databaseId, tableId, actingPrincipal)")
  protected ApiResponse<Void> auditDeleteTable(
      ProceedingJoinPoint point, String databaseId, String tableId, String actingPrincipal)
      throws Throwable {
    ApiResponse<Void> result = null;
    TableAuditEvent event =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .databaseName(databaseId)
            .tableName(tableId)
            .operationType(OperationType.DELETE)
            .build();
    try {
      result = (ApiResponse<Void>) point.proceed();
      buildAndSendEvent(event, OperationStatus.SUCCESS, null);
    } catch (Throwable t) {
      buildAndSendEvent(event, OperationStatus.FAILED, null);
      throw t;
    }
    return result;
  }

  /** Install the Around advice for deleteTable() method in OpenHouseTablesApiHandler */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<Void> "
          + "com.linkedin.openhouse.tables.api.handler.TablesApiHandler.renameTable(..)) "
          + "&& args(fromDatabaseId, fromTableId, toDatabaseId, toTableId, actingPrincipal)")
  protected ApiResponse<Void> auditRenameTable(
      ProceedingJoinPoint point,
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String actingPrincipal)
      throws Throwable {
    ApiResponse<Void> result = null;
    TableAuditEvent fromEvent =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .databaseName(fromDatabaseId)
            .tableName(fromTableId)
            .operationType(OperationType.RENAME_FROM)
            .build();
    TableAuditEvent toEvent =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .databaseName(toDatabaseId)
            .tableName(toTableId)
            .operationType(OperationType.RENAME_TO)
            .build();
    try {
      result = (ApiResponse<Void>) point.proceed();
      buildAndSendEvent(fromEvent, OperationStatus.SUCCESS, null);
      buildAndSendEvent(toEvent, OperationStatus.SUCCESS, null);
    } catch (Throwable t) {
      buildAndSendEvent(fromEvent, OperationStatus.FAILED, null);
      throw t;
    }
    return result;
  }

  /** Install the Around advice for updateAclPolicies() method in OpenHouseTablesApiHandler */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<Void> "
          + "com.linkedin.openhouse.tables.api.handler.TablesApiHandler.updateAclPolicies(..)) "
          + "&& args(databaseId, tableId, updateAclPoliciesRequestBody, actingPrincipal)")
  protected ApiResponse<Void> auditUpdateAclPolicies(
      ProceedingJoinPoint point,
      String databaseId,
      String tableId,
      UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody,
      String actingPrincipal)
      throws Throwable {
    ApiResponse<Void> result = null;
    OperationType operationType =
        updateAclPoliciesRequestBody.getOperation() == UpdateAclPoliciesRequestBody.Operation.GRANT
            ? OperationType.GRANT
            : OperationType.REVOKE;
    TableAuditEvent event =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .databaseName(databaseId)
            .tableName(tableId)
            .operationType(operationType)
            .grantor(actingPrincipal)
            .grantee(updateAclPoliciesRequestBody.getPrincipal())
            .role(updateAclPoliciesRequestBody.getRole())
            .build();
    try {
      result = (ApiResponse<Void>) point.proceed();
      buildAndSendEvent(event, OperationStatus.SUCCESS, null);
    } catch (Throwable t) {
      buildAndSendEvent(event, OperationStatus.FAILED, null);
      throw t;
    }
    return result;
  }

  /** Install the Around advice for getAclPolicies() method in OpenHouseTablesApiHandler */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody> "
          + "com.linkedin.openhouse.tables.api.handler.TablesApiHandler.getAclPolicies(..)) "
          + "&& args(databaseId, tableId, actingPrincipal)")
  protected ApiResponse<GetAclPoliciesResponseBody> auditGetAclPolicies(
      ProceedingJoinPoint point, String databaseId, String tableId, String actingPrincipal)
      throws Throwable {
    ApiResponse<GetAclPoliciesResponseBody> result = null;
    TableAuditEvent event =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .databaseName(databaseId)
            .tableName(tableId)
            .operationType(OperationType.READ)
            .build();
    try {
      result = (ApiResponse<GetAclPoliciesResponseBody>) point.proceed();
      buildAndSendEvent(event, OperationStatus.SUCCESS, null);
    } catch (Throwable t) {
      buildAndSendEvent(event, OperationStatus.FAILED, null);
      throw t;
    }
    return result;
  }

  /**
   * Configure Around advice for getAclPoliciesForUserPrincipal() method in {@link
   * OpenHouseTablesApiHandler} to audit the response.
   */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody> "
          + "com.linkedin.openhouse.tables.api.handler.TablesApiHandler.getAclPoliciesForUserPrincipal(..)) "
          + "&& args(databaseId, tableId, actingPrincipal, userPrincipal)")
  protected ApiResponse<GetAclPoliciesResponseBody> auditGetAclPoliciesForUserPrincipal(
      ProceedingJoinPoint point,
      String databaseId,
      String tableId,
      String actingPrincipal,
      String userPrincipal)
      throws Throwable {
    ApiResponse<GetAclPoliciesResponseBody> result = null;
    TableAuditEvent event =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .databaseName(databaseId)
            .tableName(tableId)
            .operationType(OperationType.READ)
            .build();
    try {
      result = (ApiResponse<GetAclPoliciesResponseBody>) point.proceed();
      buildAndSendEvent(event, OperationStatus.SUCCESS, null);
    } catch (Throwable t) {
      buildAndSendEvent(event, OperationStatus.FAILED, null);
      throw t;
    }
    return result;
  }

  /**
   * Install the Around advice for putIcebergSnapshots() method in
   * OpenHouseIcebergSnapshotsApiHandler
   */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody> "
          + "com.linkedin.openhouse.tables.api.handler.IcebergSnapshotsApiHandler.putIcebergSnapshots(..)) "
          + "&& args(databaseId, tableId, icebergSnapshotRequestBody, tableCreator)")
  protected ApiResponse<GetTableResponseBody> auditPutIcebergSnapshots(
      ProceedingJoinPoint point,
      String databaseId,
      String tableId,
      IcebergSnapshotsRequestBody icebergSnapshotRequestBody,
      String tableCreator)
      throws Throwable {
    ApiResponse<GetTableResponseBody> result = null;
    OperationType operationType = null;
    if (icebergSnapshotRequestBody.getCreateUpdateTableRequestBody().isReplaceCommit()) {
      operationType = OperationType.REPLACE_COMMIT;
    } else if (icebergSnapshotRequestBody.getBaseTableVersion().equals(INITIAL_TABLE_VERSION)) {
      operationType = OperationType.STAGED_COMMIT;
    } else {
      operationType = OperationType.COMMIT;
    }
    TableAuditEvent.TableAuditEventBuilder eventBuilder =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .databaseName(databaseId)
            .tableName(tableId)
            .operationType(operationType);
    extractSnapshotInfo(icebergSnapshotRequestBody, eventBuilder);
    try {
      result = (ApiResponse<GetTableResponseBody>) point.proceed();
      // Read tableProperties from the response, not the request body: OpenHouse mutates
      // properties server-side during commit (e.g. openhouse.tableVersion,
      // openhouse.lastModifiedTime), and the audit event should reflect the committed state.
      TableAuditEvent event =
          eventBuilder
              .tableProperties(filterTableProperties(result.getResponseBody().getTableProperties()))
              .build();
      buildAndSendEvent(
          event, OperationStatus.SUCCESS, result.getResponseBody().getTableLocation());
    } catch (Throwable t) {
      // On failure there is no committed state to read from, so tableProperties stays null.
      buildAndSendEvent(eventBuilder.build(), OperationStatus.FAILED, null);
      throw t;
    }
    return result;
  }

  /**
   * Extracts snapshot ID, timestamp, and branch ref name from the request body.
   *
   * <p>branchRefName is the ref whose snapshot-id matches the last snapshot in jsonSnapshots.
   * Iceberg appends snapshots chronologically, so the last one is always the newly-committed
   * snapshot, and the ref pointing to it is the branch that was written.
   *
   * <p>currentSnapshotId and currentSnapshotTimestampMs track the main branch ref for backwards
   * compatibility. They are null when main is absent from snapshotRefs.
   */
  private void extractSnapshotInfo(
      IcebergSnapshotsRequestBody requestBody,
      TableAuditEvent.TableAuditEventBuilder eventBuilder) {
    try {
      Map<String, String> snapshotRefs = requestBody.getSnapshotRefs();
      List<String> jsonSnapshots = requestBody.getJsonSnapshots();
      if (snapshotRefs == null || jsonSnapshots == null || jsonSnapshots.isEmpty()) {
        return;
      }

      // The last snapshot in the list is the one being committed (Iceberg appends chronologically).
      // Find which ref points to it — that is the branch being written.
      long lastSnapshotId =
          SnapshotParser.fromJson(jsonSnapshots.get(jsonSnapshots.size() - 1)).snapshotId();
      for (Map.Entry<String, String> entry : snapshotRefs.entrySet()) {
        if (SnapshotRefParser.fromJson(entry.getValue()).snapshotId() == lastSnapshotId) {
          eventBuilder.branchRefName(entry.getKey());
          break;
        }
      }

      // Extract snapshot ID and timestamp for main branch (backwards-compatible).
      // Iterate jsonSnapshots in reverse: main's snapshot is typically the most recent.
      String mainRefJson = snapshotRefs.get(SnapshotRef.MAIN_BRANCH);
      if (mainRefJson == null) {
        return;
      }
      long mainSnapshotId = SnapshotRefParser.fromJson(mainRefJson).snapshotId();
      eventBuilder.currentSnapshotId(mainSnapshotId);

      String mainSnapshotIdStr = Long.toString(mainSnapshotId);
      for (int i = jsonSnapshots.size() - 1; i >= 0; i--) {
        String snapshotJson = jsonSnapshots.get(i);
        if (!snapshotJson.contains(mainSnapshotIdStr)) {
          continue;
        }
        Snapshot snapshot = SnapshotParser.fromJson(snapshotJson);
        if (snapshot.snapshotId() == mainSnapshotId) {
          eventBuilder.currentSnapshotTimestampMs(snapshot.timestampMillis());
          return;
        }
      }
    } catch (Exception e) {
      // Snapshot extraction is best-effort; don't fail the audit event
      log.warn("Failed to extract snapshot info for audit event", e);
    }
  }

  /** Install the Around advice for getAllDatabases() method in OpenHouseDatabasesApiHandler */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<com.linkedin.openhouse.tables.api.spec.v0.response.GetAllDatabasesResponseBody> "
          + "com.linkedin.openhouse.tables.api.handler.DatabasesApiHandler.getAllDatabases(..))")
  protected ApiResponse<GetAllDatabasesResponseBody> auditGetAllDatabases(ProceedingJoinPoint point)
      throws Throwable {
    ApiResponse<GetAllDatabasesResponseBody> result = null;
    TableAuditEvent event =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .operationType(OperationType.READ)
            .build();
    try {
      result = (ApiResponse<GetAllDatabasesResponseBody>) point.proceed();
      buildAndSendEvent(event, OperationStatus.SUCCESS, null);
    } catch (Throwable t) {
      buildAndSendEvent(event, OperationStatus.FAILED, null);
      throw t;
    }
    return result;
  }

  /**
   * Install the Around advice for getDatabaseAclPolicies() method in OpenHouseDatabasesApiHandler
   */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody> "
          + "com.linkedin.openhouse.tables.api.handler.DatabasesApiHandler.getDatabaseAclPolicies(..)) "
          + "&& args(databaseId, actingPrincipal)")
  protected ApiResponse<GetAclPoliciesResponseBody> auditGetDatabaseAclPolicies(
      ProceedingJoinPoint point, String databaseId, String actingPrincipal) throws Throwable {
    ApiResponse<GetAclPoliciesResponseBody> result = null;
    TableAuditEvent event =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .databaseName(databaseId)
            .operationType(OperationType.READ)
            .build();
    try {
      result = (ApiResponse<GetAclPoliciesResponseBody>) point.proceed();
      buildAndSendEvent(event, OperationStatus.SUCCESS, null);
    } catch (Throwable t) {
      buildAndSendEvent(event, OperationStatus.FAILED, null);
      throw t;
    }
    return result;
  }

  /**
   * Install the Around advice for updateDatabaseAclPolicies() method in
   * OpenHouseDatabasesApiHandler
   */
  @Around(
      "execution("
          + "com.linkedin.openhouse.common.api.spec.ApiResponse<Void> "
          + "com.linkedin.openhouse.tables.api.handler.DatabasesApiHandler.updateDatabaseAclPolicies(..)) "
          + "&& args(databaseId, updateAclPoliciesRequestBody, actingPrincipal)")
  protected ApiResponse<Void> auditUpdateDatabaseAclPolicies(
      ProceedingJoinPoint point,
      String databaseId,
      UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody,
      String actingPrincipal)
      throws Throwable {
    ApiResponse<Void> result = null;
    OperationType operationType =
        updateAclPoliciesRequestBody.getOperation() == UpdateAclPoliciesRequestBody.Operation.GRANT
            ? OperationType.GRANT
            : OperationType.REVOKE;
    TableAuditEvent event =
        TableAuditEvent.builder()
            .eventTimestamp(Instant.now())
            .databaseName(databaseId)
            .operationType(operationType)
            .grantor(actingPrincipal)
            .grantee(updateAclPoliciesRequestBody.getPrincipal())
            .role(updateAclPoliciesRequestBody.getRole())
            .build();
    try {
      result = (ApiResponse<Void>) point.proceed();
      buildAndSendEvent(event, OperationStatus.SUCCESS, null);
    } catch (Throwable t) {
      buildAndSendEvent(event, OperationStatus.FAILED, null);
      throw t;
    }
    return result;
  }

  /**
   * Narrows the committed table properties down to the configured allowlist ({@code
   * cluster.iceberg.tables.audit.table-properties-allowlist}). Returns {@code null} when there is
   * nothing to emit so downstream audit handlers can skip the field entirely. Iterates the
   * allowlist rather than the source so cost is O(|allowlist|) regardless of source size.
   */
  private Map<String, String> filterTableProperties(Map<String, String> source) {
    if (source == null || source.isEmpty()) {
      return null;
    }
    List<String> allowlist = internalCatalogProperties.getAudit().getTablePropertiesAllowlist();
    if (allowlist == null || allowlist.isEmpty()) {
      return null;
    }
    Map<String, String> filtered = new HashMap<>();
    for (String key : allowlist) {
      String value = source.get(key);
      if (value != null) {
        filtered.put(key, value);
      }
    }
    return filtered.isEmpty() ? null : filtered;
  }

  private void buildAndSendEvent(
      TableAuditEvent event, OperationStatus status, String currentTableRoot) {
    TableAuditEvent completeEvent =
        event
            .toBuilder()
            .clusterName(clusterProperties.getClusterName())
            .user(extractAuthenticatedUserPrincipal())
            .operationStatus(status)
            .currentTableRoot(currentTableRoot)
            .build();
    tableAuditHandler.audit(completeEvent);
  }
}
