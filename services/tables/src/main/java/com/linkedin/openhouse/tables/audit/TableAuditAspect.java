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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefParser;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

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
   * Cached compiled allowlist patterns. Built lazily on first use from {@code
   * cluster.iceberg.tables.audit.table-properties-allowlist}. The aspect is a singleton and the
   * config is bound once at startup, so a one-shot lazy init is sufficient.
   */
  private volatile List<Pattern> allowlistPatterns;

  // Iceberg serializes a snapshot's operation as a separate top-level field rather than inside the
  // summary map (SnapshotParser parses it out into Snapshot#operation), so there is no public
  // constant for this key. We merge it back into the emitted summary under this name to mirror the
  // on-disk JSON.
  private static final String SNAPSHOT_OPERATION_KEY = "operation";

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
              .auditedTableProperties(
                  filterTableProperties(result.getResponseBody().getTableProperties()))
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
   * Extracts snapshot ID and timestamp of the main branch from the request body. The snapshotRefs
   * map contains branch name to JSON-serialized SnapshotRef. We read the main branch's snapshot-id
   * (this is what Iceberg treats as current-snapshot-id — see TableMetadata.Builder.setRef()) and
   * then find the matching snapshot in jsonSnapshots to get its timestamp-ms.
   *
   * <p>Leaves both fields null if the main branch ref is absent (e.g. branch-only commits where
   * main didn't advance, or non-commit operations) or if the matching snapshot can't be found.
   */
  private void extractSnapshotInfo(
      IcebergSnapshotsRequestBody requestBody,
      TableAuditEvent.TableAuditEventBuilder eventBuilder) {
    try {
      Map<String, String> snapshotRefs = requestBody.getSnapshotRefs();
      if (snapshotRefs == null) {
        return;
      }
      String mainRefJson = snapshotRefs.get(SnapshotRef.MAIN_BRANCH);
      if (mainRefJson == null) {
        return;
      }
      long mainSnapshotId = SnapshotRefParser.fromJson(mainRefJson).snapshotId();
      eventBuilder.currentSnapshotId(mainSnapshotId);

      // Find the matching snapshot in jsonSnapshots to get its timestamp-ms. Iterate in reverse
      // because Iceberg appends snapshots chronologically and main's snapshot is typically the
      // most recent. Skip snapshots whose JSON doesn't contain the target id as a cheap
      // pre-filter before invoking the JSON parser.
      List<String> jsonSnapshots = requestBody.getJsonSnapshots();
      if (jsonSnapshots == null) {
        return;
      }
      String mainSnapshotIdStr = Long.toString(mainSnapshotId);
      for (int i = jsonSnapshots.size() - 1; i >= 0; i--) {
        String snapshotJson = jsonSnapshots.get(i);
        if (!snapshotJson.contains(mainSnapshotIdStr)) {
          continue;
        }
        Snapshot snapshot = SnapshotParser.fromJson(snapshotJson);
        if (snapshot.snapshotId() == mainSnapshotId) {
          eventBuilder.currentSnapshotTimestampMs(snapshot.timestampMillis());
          eventBuilder.snapshotSummary(buildSnapshotSummary(snapshot));
          return;
        }
      }
    } catch (Exception e) {
      // Snapshot extraction is best-effort; don't fail the audit event
      log.warn("Failed to extract snapshot info for audit event", e);
    }
  }

  /**
   * Builds the emitted snapshot summary: the snapshot's own summary map plus its operation (which
   * Iceberg parses out of the summary into a separate field), mirroring the on-disk JSON.
   */
  private static Map<String, String> buildSnapshotSummary(Snapshot snapshot) {
    Map<String, String> summary = new HashMap<>();
    if (snapshot.summary() != null) {
      summary.putAll(snapshot.summary());
    }
    if (snapshot.operation() != null) {
      summary.put(SNAPSHOT_OPERATION_KEY, snapshot.operation());
    }
    return summary;
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
   * cluster.iceberg.tables.audit.table-properties-allowlist}) and enforces two byte-size caps to
   * keep the audit event within the Kafka producer's max.request.size budget: a per-value cap
   * ({@code table-property-value-max-size}) and a combined-total cap ({@code
   * table-properties-total-max-size}). Values exceeding either cap are skipped with a warning log.
   * Returns {@code null} when there is nothing to emit so downstream audit handlers can skip the
   * field entirely.
   *
   * <p>Allowlist entries are Java regular expressions matched against the property key (full match,
   * via {@link Pattern#matches}); a key is emitted if it matches at least one pattern. Invalid
   * patterns are logged and skipped — they never block audit emission. Source keys are visited in
   * sorted order so that the total-cap cutoff is deterministic regardless of the source map's
   * iteration order.
   */
  private Map<String, String> filterTableProperties(Map<String, String> source) {
    if (source == null || source.isEmpty()) {
      return null;
    }
    List<Pattern> patterns = compiledAllowlistPatterns();
    if (patterns.isEmpty()) {
      return null;
    }
    InternalCatalogProperties.Audit auditConfig = internalCatalogProperties.getAudit();
    long maxValueBytes = auditConfig.getTablePropertyValueMaxSize().toBytes();
    long maxTotalBytes = auditConfig.getTablePropertiesTotalMaxSize().toBytes();
    // Sort keys so the greedy total-byte cap below drops the same properties every run; the source
    // HashMap has no stable order. Irrelevant when the cap isn't hit.
    List<String> sortedKeys = new ArrayList<>(source.keySet());
    Collections.sort(sortedKeys);
    Map<String, String> filtered = new HashMap<>();
    long totalBytes = 0;
    for (String key : sortedKeys) {
      if (!matchesAnyPattern(key, patterns)) {
        continue;
      }
      String value = source.get(key);
      if (value == null) {
        continue;
      }
      long valueBytes = value.getBytes(StandardCharsets.UTF_8).length;
      if (valueBytes > maxValueBytes) {
        log.warn(
            "Dropping audited table-property '{}': value size {} bytes exceeds per-value cap {} bytes",
            key,
            valueBytes,
            maxValueBytes);
        continue;
      }
      if (totalBytes + valueBytes > maxTotalBytes) {
        log.warn(
            "Dropping audited table-property '{}': including it would exceed total cap {} bytes "
                + "(already accumulated {} bytes)",
            key,
            maxTotalBytes,
            totalBytes);
        continue;
      }
      filtered.put(key, value);
      totalBytes += valueBytes;
    }
    return filtered.isEmpty() ? null : filtered;
  }

  /** Returns {@code true} if {@code key} fully matches at least one allowlist pattern. */
  private boolean matchesAnyPattern(String key, List<Pattern> patterns) {
    for (Pattern pattern : patterns) {
      if (pattern.matcher(key).matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Compiles the configured allowlist into {@link Pattern}s once and caches the result. Returns
   * {@link Collections#emptyList()} when the allowlist is unset or every pattern is invalid.
   */
  private List<Pattern> compiledAllowlistPatterns() {
    List<Pattern> cached = allowlistPatterns;
    if (cached != null) {
      return cached;
    }
    List<String> allowlist = internalCatalogProperties.getAudit().getTablePropertiesAllowlist();
    if (allowlist == null || allowlist.isEmpty()) {
      allowlistPatterns = Collections.emptyList();
      return allowlistPatterns;
    }
    List<Pattern> compiled = new ArrayList<>(allowlist.size());
    for (String regex : allowlist) {
      try {
        compiled.add(Pattern.compile(regex));
      } catch (PatternSyntaxException e) {
        log.warn("Skipping invalid table-property allowlist regex '{}': {}", regex, e.getMessage());
      }
    }
    allowlistPatterns = Collections.unmodifiableList(compiled);
    return allowlistPatterns;
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
            .clientUserAgent(extractUserAgent())
            .build();
    tableAuditHandler.audit(completeEvent);
  }

  /**
   * Reads the raw {@code User-Agent} request header verbatim, or null when no servlet request is
   * bound to the current thread (e.g. async or test contexts). The value is stored unparsed so the
   * client runtime version can be extracted at query time. Best-effort: a failure here must never
   * disrupt the audited operation. Mirrors the request access in {@link
   * com.linkedin.openhouse.common.audit.ServiceAuditAspect}.
   */
  private String extractUserAgent() {
    try {
      RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
      if (requestAttributes instanceof ServletRequestAttributes) {
        HttpServletRequest request = ((ServletRequestAttributes) requestAttributes).getRequest();
        return request.getHeader(HttpHeaders.USER_AGENT);
      }
    } catch (Exception e) {
      log.warn("Failed to read User-Agent header for audit event", e);
    }
    return null;
  }
}
