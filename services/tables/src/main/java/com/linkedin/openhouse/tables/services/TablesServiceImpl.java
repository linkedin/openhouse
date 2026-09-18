package com.linkedin.openhouse.tables.services;

import static com.linkedin.openhouse.common.utils.PageableUtil.createPageable;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.common.exception.OpenHouseCommitStateUnknownException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTableDto;
import com.linkedin.openhouse.internal.catalog.model.SoftDeletedTablePrimaryKey;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.response.components.AclPolicy;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.readbridge.ColumnDefaultException;
import com.linkedin.openhouse.tables.readbridge.ReadBridgeStripProtection;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import com.linkedin.openhouse.tables.utils.TableUUIDGenerator;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

/** Default Table Service Implementation for /tables REST endpoint. */
@Component
public class TablesServiceImpl implements TablesService {

  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired TablesMapper tablesMapper;

  @Autowired AuthorizationUtils authorizationUtils;

  @Autowired AuthorizationHandler authorizationHandler;

  @Autowired TableUUIDGenerator tableUUIDGenerator;

  @Autowired ReadBridgeStripProtection readBridgeStripProtection;
  /**
   * Lookup a table by databaseId and tableId in OpenHouse's Internal Catalog.
   *
   * @param databaseId
   * @param tableId
   * @return Table DTO object.
   * @throws com.linkedin.openhouse.common.exception.NoSuchUserTableException if table is not found.
   */
  @Override
  public TableDto getTable(String databaseId, String tableId, String actingPrincipal) {
    TableDto tableDto =
        openHouseInternalRepository
            .findById(TableDtoPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build())
            .orElseThrow(() -> new NoSuchUserTableException(databaseId, tableId));
    // Restricts reading table to users with lock admin Privileges
    if (isTableLocked(tableDto)) {
      authorizationUtils.checkLockTablePrivilege(tableDto, actingPrincipal, Privileges.LOCK_ADMIN);
    }
    authorizationUtils.checkTablePrivilege(
        tableDto, actingPrincipal, Privileges.GET_TABLE_METADATA);
    return tableDto;
  }

  @Override
  public List<TableDto> searchTables(String databaseId) {
    return openHouseInternalRepository.searchTables(databaseId);
  }

  @Override
  public Page<TableDto> searchTables(
      String databaseId,
      int page,
      int size,
      String sortBy,
      List<String> fields,
      String actingPrincipal) {
    if (fields != null && !fields.isEmpty()) {
      authorizationUtils.checkDatabasePrivilege(
          databaseId, actingPrincipal, Privileges.GET_TABLE_METADATA);
    }
    Pageable pageable = createPageable(page, size, sortBy, null);
    return openHouseInternalRepository.searchTables(databaseId, pageable, fields);
  }

  @WithSpan("TablesService.putTable")
  @Override
  public Pair<TableDto, Boolean> putTable(
      CreateUpdateTableRequestBody createUpdateTableRequestBody,
      String tableCreatorUpdater,
      Boolean failOnExist) {
    String databaseId = createUpdateTableRequestBody.getDatabaseId();
    String tableId = createUpdateTableRequestBody.getTableId();

    Optional<TableDto> tableDto =
        openHouseInternalRepository.findById(
            TableDtoPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build());

    // Special case handling
    if (tableDto.isPresent() && createUpdateTableRequestBody.isStageReplace()) {
      authorizationUtils.checkTableWritePathPrivileges(
          tableDto.get(), tableCreatorUpdater, Privileges.UPDATE_TABLE_METADATA);
    } else if (tableDto.isPresent()) {
      if (failOnExist) {
        throw new AlreadyExistsException("Table", String.format("%s.%s", databaseId, tableId));
      }
      if (tableDto.get().isStageCreate()) {
        throw new IllegalStateException(
            String.format("Staged Table %s.%s was illegally persisted", databaseId, tableId));
      }
      checkIfLockPoliciesUpdated(tableDto.get(), createUpdateTableRequestBody);
      if (isTableLocked(tableDto.get())) {
        throw new UnsupportedClientOperationException(
            UnsupportedClientOperationException.Operation.LOCKED_TABLE_OPERATION,
            String.format(
                "Table %s.%s is in locked state and cannot be updated.", databaseId, tableId));
      }
      authorizationUtils.checkTableWritePathPrivileges(
          tableDto.get(), tableCreatorUpdater, Privileges.UPDATE_TABLE_METADATA);

      // An optimization to avoid persisting unchanged TableDto into HouseTable.
      if (!updateNeeded(tableDto.get(), createUpdateTableRequestBody)) {
        return Pair.of(tableDto.get(), /*creation didn't occur*/ false);
      }
    } else {
      // Check if table creator has the privilege to create a table in this DB.
      authorizationUtils.checkDatabasePrivilege(
          databaseId, tableCreatorUpdater, Privileges.CREATE_TABLE);
    }

    // FIXME: save method redundantly issue existence check after findById is called above
    TableDto tableDtoToSave =
        tablesMapper.toTableDto(
            tableDto.orElseGet(
                () ->
                    TableDto.builder()
                        .tableUri(
                            TableUri.builder()
                                .tableId(tableId)
                                .databaseId(databaseId)
                                .clusterId(createUpdateTableRequestBody.getClusterId())
                                .build()
                                .toString())
                        .tableUUID(
                            tableUUIDGenerator
                                .generateUUID(createUpdateTableRequestBody)
                                .toString())
                        .tableCreator(tableCreatorUpdater)
                        .build()),
            createUpdateTableRequestBody);
    try {
      tableDtoToSave = readBridgeStripProtection.prepare(tableDto.orElse(null), tableDtoToSave);
    } catch (ColumnDefaultException e) {
      throw e.toUnsupportedClient();
    }
    return saveTableDto(tableDtoToSave, tableDto);
  }

  private Pair<TableDto, Boolean> saveTableDto(
      TableDto tableDtoToSave, Optional<TableDto> tableDto) {
    try {
      return Pair.of(openHouseInternalRepository.save(tableDtoToSave), !tableDto.isPresent());
    } catch (BadRequestException e) {
      throw new RequestValidationFailureException(e.getMessage(), e);
    } catch (CommitFailedException ce) {
      throw new EntityConcurrentModificationException(
          tableDtoToSave.getTableUri(),
          String.format(
              "databaseId : %s, tableId : %s, version: %s %s",
              tableDtoToSave.getDatabaseId(),
              tableDtoToSave.getTableId(),
              tableDtoToSave.getTableVersion(),
              "The requested table has been modified/created by other processes."),
          ce);
    } catch (CommitStateUnknownException commitStateUnknownException) {
      throw new OpenHouseCommitStateUnknownException(
          tableDtoToSave.getTableUri(),
          String.format(
              "databaseId : %s, tableId : %s, version: %s %s",
              tableDtoToSave.getDatabaseId(),
              tableDtoToSave.getTableId(),
              tableDtoToSave.getTableVersion(),
              "Commit regarding to the requested table is not acknowledged."),
          commitStateUnknownException);
    }
  }

  private void checkIfLockPoliciesUpdated(
      TableDto tableDto, CreateUpdateTableRequestBody requestBody) {
    if (requestBody.getPolicies() != null
        && requestBody.getPolicies().getLockState() != null
        && requestBody.getPolicies().getLockState().isLocked()
            != tableDto.getPolicies().getLockState().isLocked()) {
      throw new IllegalArgumentException(
          String.format(
              "Lock state cannot be updated for Table %s.%s",
              tableDto.getDatabaseId(), tableDto.getTableId()));
    }
  }

  /** Return true if update is needed. */
  private boolean updateNeeded(
      TableDto existingTableDto, CreateUpdateTableRequestBody requestBody) {
    return !tablesMapper.toTableDto(existingTableDto, requestBody).equals(existingTableDto);
  }

  @Override
  public void deleteTable(String databaseId, String tableId, String actingPrincipal) {
    TableDtoPrimaryKey tableDtoPrimaryKey =
        TableDtoPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build();

    // Table-ref lookup (no metadata.json parse) is enough here — drop only needs identifiers +
    // tableUUID for the ACL check. Lets us drop tables whose metadata.json is corrupted.
    TableDto tableDto =
        openHouseInternalRepository
            .findTableRefById(tableDtoPrimaryKey)
            .orElseThrow(() -> new NoSuchUserTableException(databaseId, tableId));

    authorizationUtils.checkTableDropPrivilege(tableDto, actingPrincipal, Privileges.DELETE_TABLE);

    openHouseInternalRepository.deleteById(tableDtoPrimaryKey);
  }

  @Override
  public void renameTable(
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String tableCreatorUpdater) {
    Optional<TableDto> existingTableDto =
        openHouseInternalRepository.findById(
            TableDtoPrimaryKey.builder().databaseId(fromDatabaseId).tableId(fromTableId).build());

    if (!existingTableDto.isPresent()) {
      throw new NoSuchUserTableException(fromDatabaseId, fromTableId);
    }

    Optional<TableDto> targetedTableDto =
        openHouseInternalRepository.findById(
            TableDtoPrimaryKey.builder().databaseId(toDatabaseId).tableId(toTableId).build());
    if (targetedTableDto.isPresent()) {
      throw new AlreadyExistsException("Table", targetedTableDto.get().getTableUri());
    }

    if (isTableLocked(existingTableDto.get())) {
      throw new UnsupportedClientOperationException(
          UnsupportedClientOperationException.Operation.LOCKED_TABLE_OPERATION,
          String.format(
              "Table %s.%s is in locked state and cannot be renamed.",
              fromDatabaseId, fromTableId));
    }
    // Rename involves both modifying an existing table and creating a new one
    authorizationUtils.checkDatabasePrivilege(
        fromDatabaseId, tableCreatorUpdater, Privileges.CREATE_TABLE);
    authorizationUtils.checkTableWritePathPrivileges(
        existingTableDto.get(), tableCreatorUpdater, Privileges.UPDATE_TABLE_METADATA);

    openHouseInternalRepository.rename(
        TableDtoPrimaryKey.builder().databaseId(fromDatabaseId).tableId(fromTableId).build(),
        TableDtoPrimaryKey.builder().databaseId(toDatabaseId).tableId(toTableId).build());
  }

  @Override
  public void updateAclPolicies(
      String databaseId,
      String tableId,
      UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody,
      String actingPrincipal) {
    TableDto tableDto = getTableOrThrow(databaseId, tableId);
    authorizationUtils.checkTablePrivilege(tableDto, actingPrincipal, Privileges.UPDATE_ACL);

    String role = updateAclPoliciesRequestBody.getRole();
    String granteePrincipal = updateAclPoliciesRequestBody.getPrincipal();
    Long expirationEpochTimeSeconds = updateAclPoliciesRequestBody.getExpirationEpochTimeSeconds();
    Map<String, String> properties = updateAclPoliciesRequestBody.getProperties();

    switch (updateAclPoliciesRequestBody.getOperation()) {
      case GRANT:
        if (!isTableSharingEnabled(tableDto)) {
          throw new UnsupportedClientOperationException(
              UnsupportedClientOperationException.Operation.GRANT_ON_UNSHARED_TABLES,
              String.format("%s.%s is not a shared table", databaseId, tableId));
        }
        if (isTableLocked(tableDto)) {
          throw new UnsupportedClientOperationException(
              UnsupportedClientOperationException.Operation.GRANT_ON_LOCKED_TABLES,
              String.format(
                  "%s.%s is in locked state and grants are not allowed for sharing",
                  databaseId, tableId));
        }
        authorizationHandler.grantRole(
            role,
            granteePrincipal,
            expirationEpochTimeSeconds,
            properties,
            tableDto,
            actingPrincipal);
        break;
      case REVOKE:
        authorizationHandler.revokeRole(role, granteePrincipal, tableDto, actingPrincipal);
        break;
      default:
        throw new UnsupportedOperationException("Only GRANT and REVOKE are supported");
    }
  }

  @Override
  public List<AclPolicy> getAclPolicies(String databaseId, String tableId, String actingPrincipal) {
    TableDto tableDto = getTableOrThrow(databaseId, tableId);
    return authorizationHandler.listAclPolicies(tableDto);
  }

  @Override
  public List<AclPolicy> getAclPolicies(
      String databaseId, String tableId, String actingPrincipal, String userPrincipal) {
    TableDto tableDto = getTableOrThrow(databaseId, tableId);
    return authorizationHandler.listAclPolicies(tableDto, userPrincipal);
  }

  /**
   * Creates a lock on a table. A LEGACY request records an unqualified lock and overwrites an
   * existing LEGACY lock. A structured reason additionally records the acting principal as the lock
   * owner and pins the lock to the current table generation, so it requires expectedTableUUID and
   * an authenticated principal. Repeating a structured request that matches the active lock leaves
   * that lock in place.
   *
   * @param databaseId
   * @param tableId
   * @param createUpdateLockRequestBody
   * @param tableCreatorUpdater
   */
  @Override
  public void createLock(
      String databaseId,
      String tableId,
      CreateUpdateLockRequestBody createUpdateLockRequestBody,
      String tableCreatorUpdater) {
    TableDto tableDto = authorizeLockOperation(databaseId, tableId, tableCreatorUpdater);
    LockReason requestedReason = createUpdateLockRequestBody.getReason();
    if (requestedReason != LockReason.LEGACY) {
      if (StringUtils.isBlank(tableCreatorUpdater)) {
        throw new RequestValidationFailureException(
            "An authenticated lock owner is required for a lock with reason " + requestedReason);
      }
      checkLockTableGeneration(tableDto, createUpdateLockRequestBody.getExpectedTableUUID());
    }
    if (!createUpdateLockRequestBody.isLocked()) {
      return;
    }

    Optional<LockState> activeLock = findActiveLock(tableDto);
    // A LEGACY request refreshes an active LEGACY lock. Once either side carries a structured
    // reason the active lock is protected: a repeat of the same reason, owner, and generation
    // keeps it in place, and every other request is rejected.
    if (activeLock.isPresent()
        && (requestedReason != LockReason.LEGACY
            || activeLock.get().getReason() != LockReason.LEGACY)) {
      if (requestedReason == activeLock.get().getReason()
          && tableCreatorUpdater.equals(activeLock.get().getLockOwner())
          && tableDto.getTableUUID().equals(activeLock.get().getTableUUID())) {
        return;
      }
      throw lockMismatch(tableDto);
    }

    LockState.LockStateBuilder lockStateBuilder =
        LockState.builder()
            .locked(true)
            .message(createUpdateLockRequestBody.getMessage())
            .reason(requestedReason)
            .expirationInDays(createUpdateLockRequestBody.getExpirationInDays())
            .creationTime(createUpdateLockRequestBody.getCreationTime());
    if (requestedReason != LockReason.LEGACY) {
      lockStateBuilder.lockOwner(tableCreatorUpdater).tableUUID(tableDto.getTableUUID());
    }
    LockState lockState = lockStateBuilder.build();
    Policies policiesToSave =
        Optional.ofNullable(tableDto.getPolicies())
            .map(policies -> policies.toBuilder().lockState(lockState).build())
            .orElseGet(() -> Policies.builder().lockState(lockState).build());
    savePolicies(tableDto, policiesToSave);
  }

  /**
   * Removes a LEGACY lock from the table. A table whose active lock carries a structured reason
   * keeps that lock and the request is rejected. A table without an active lock is left unchanged.
   *
   * @param databaseId
   * @param tableId
   * @param actingPrincipal
   */
  @Override
  public void deleteLock(String databaseId, String tableId, String actingPrincipal) {
    TableDto tableDto = authorizeLockOperation(databaseId, tableId, actingPrincipal);
    Optional<LockState> activeLock = findActiveLock(tableDto);
    if (!activeLock.isPresent()) {
      return;
    }
    if (activeLock.get().getReason() != LockReason.LEGACY) {
      throw lockMismatch(tableDto);
    }
    clearLockState(tableDto);
  }

  @Override
  public void deleteLock(
      String databaseId,
      String tableId,
      String actingPrincipal,
      LockReason reason,
      String expectedTableUUID,
      String lockOwner) {
    if (reason == LockReason.LEGACY) {
      throw new RequestValidationFailureException(
          "A reason-qualified unlock requires a structured reason; use the unqualified unlock to "
              + "remove a LEGACY lock");
    }
    if (StringUtils.isBlank(lockOwner)) {
      throw new RequestValidationFailureException(
          "lockOwner is required for an unlock with reason " + reason);
    }
    TableDto tableDto = authorizeLockOperation(databaseId, tableId, actingPrincipal);
    checkLockTableGeneration(tableDto, expectedTableUUID);

    Optional<LockState> activeLock = findActiveLock(tableDto);
    if (!activeLock.isPresent()) {
      return;
    }
    if (activeLock.get().getReason() != reason
        || !expectedTableUUID.equals(activeLock.get().getTableUUID())
        || !lockOwner.equals(activeLock.get().getLockOwner())) {
      throw lockMismatch(tableDto);
    }
    clearLockState(tableDto);
  }

  private TableDto authorizeLockOperation(
      String databaseId, String tableId, String actingPrincipal) {
    TableDto tableDto =
        openHouseInternalRepository
            .findById(TableDtoPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build())
            .orElseThrow(() -> new NoSuchUserTableException(databaseId, tableId));
    checkReplicaTable(tableDto);
    authorizationUtils.checkLockTablePrivilege(tableDto, actingPrincipal, Privileges.LOCK_ADMIN);
    return tableDto;
  }

  private Optional<LockState> findActiveLock(TableDto tableDto) {
    return Optional.ofNullable(tableDto.getPolicies())
        .map(Policies::getLockState)
        .filter(LockState::isLocked);
  }

  private void clearLockState(TableDto tableDto) {
    savePolicies(tableDto, tableDto.getPolicies().toBuilder().lockState(null).build());
  }

  private void savePolicies(TableDto tableDto, Policies policies) {
    saveTableDto(
        tableDto.toBuilder().policies(policies).tableVersion(tableDto.getTableLocation()).build(),
        Optional.of(tableDto));
  }

  private void checkLockTableGeneration(TableDto tableDto, String expectedTableUUID) {
    if (StringUtils.isBlank(expectedTableUUID)) {
      throw new RequestValidationFailureException(
          "expectedTableUUID is required for a lock operation with a structured reason");
    }
    if (!expectedTableUUID.equals(tableDto.getTableUUID())) {
      throw new AlreadyExistsException(
          "Table",
          tableDto.getTableUri(),
          "Lock operation targets a different table generation",
          null);
    }
  }

  private AlreadyExistsException lockMismatch(TableDto tableDto) {
    return new AlreadyExistsException(
        "Lock",
        tableDto.getTableUri(),
        "Existing lock does not match the requested reason, owner, and table generation",
        null);
  }

  @Override
  public Page<SoftDeletedTableDto> searchSoftDeletedTables(
      String databaseId, String tableId, int page, int size, String sortBy) {
    Pageable pageable = createPageable(page, size, sortBy, null);
    return openHouseInternalRepository.searchSoftDeletedTables(databaseId, tableId, pageable);
  }

  @Override
  public void purgeSoftDeletedTables(
      String databaseId, String tableId, long purgeAfterMs, String actingPrincipal) {
    authorizationUtils.checkDatabasePrivilege(databaseId, actingPrincipal, Privileges.DELETE_TABLE);
    openHouseInternalRepository.purgeSoftDeletedTableById(
        TableDtoPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build(), purgeAfterMs);
  }

  public void restoreTable(
      String databaseId, String tableId, long deletedAtMs, String actingPrincipal) {
    // TODO: Validation should be at the table owner level when this is self serve through SQL
    authorizationUtils.checkDatabasePrivilege(databaseId, actingPrincipal, Privileges.CREATE_TABLE);
    openHouseInternalRepository.restoreTable(
        SoftDeletedTablePrimaryKey.builder()
            .databaseId(databaseId)
            .tableId(tableId)
            .deletedAtMs(deletedAtMs)
            .build());
  }

  /** Whether sharing has been enabled for the table denoted by tableDto. */
  private boolean isTableSharingEnabled(TableDto tableDto) {
    return (tableDto.getPolicies() != null && tableDto.getPolicies().isSharingEnabled());
  }

  /**
   * Gets entity (TableDto) representing a table if exists. Else throws NoSuchUserTableException.
   *
   * @param databaseId
   * @param tableId
   * @return TableDto
   */
  private TableDto getTableOrThrow(String databaseId, String tableId) {
    TableDtoPrimaryKey tableDtoPrimaryKey =
        TableDtoPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build();

    Optional<TableDto> tableDto = openHouseInternalRepository.findById(tableDtoPrimaryKey);
    if (!tableDto.isPresent()) {
      throw new NoSuchUserTableException(databaseId, tableId);
    }
    return tableDto.get();
  }

  /**
   * Throw Exception if tableType is Replica table
   *
   * @param tableDto
   */
  private void checkReplicaTable(TableDto tableDto) {
    if (TableType.REPLICA_TABLE.equals(tableDto.getTableType())) {
      String errMsg =
          String.format(
              "Lock/UnLock Operation on Replica table %s.%s is not permitted. TableType: %s",
              tableDto.getDatabaseId(), tableDto.getTableId(), tableDto.getTableType());
      throw new UnsupportedOperationException(errMsg);
    }
  }

  /** Check if table has lock policy defined */
  private static boolean isTableLocked(TableDto tableDto) {
    return tableDto.getPolicies() != null
        && tableDto.getPolicies().getLockState() != null
        && tableDto.getPolicies().getLockState().isLocked();
  }
}
