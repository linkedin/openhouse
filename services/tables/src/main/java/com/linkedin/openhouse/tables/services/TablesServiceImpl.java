package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.common.exception.OpenHouseCommitStateUnknownException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.response.components.AclPolicy;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import com.linkedin.openhouse.tables.utils.TableUUIDGenerator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.springframework.beans.factory.annotation.Autowired;
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
    authorizationUtils.checkTablePrivilege(
        tableDto, actingPrincipal, Privileges.GET_TABLE_METADATA);
    return tableDto;
  }

  @Override
  public List<TableDto> searchTables(String databaseId) {
    return openHouseInternalRepository.searchTables(databaseId);
  }

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
    if (tableDto.isPresent()) {
      if (failOnExist) {
        throw new AlreadyExistsException("Table", String.format("%s.%s", databaseId, tableId));
      }
      if (tableDto.get().isStageCreate()) {
        throw new IllegalStateException(
            String.format("Staged Table %s.%s was illegally persisted", databaseId, tableId));
      }
      checkIfLockPoliciesUpdated(tableDto.get(), createUpdateTableRequestBody);
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

    Optional<TableDto> tableDto = openHouseInternalRepository.findById(tableDtoPrimaryKey);
    if (!tableDto.isPresent()) {
      throw new NoSuchUserTableException(databaseId, tableId);
    }
    authorizationUtils.checkTableWritePathPrivileges(
        tableDto.get(), actingPrincipal, Privileges.DELETE_TABLE);

    openHouseInternalRepository.deleteById(tableDtoPrimaryKey);
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
        authorizationHandler.grantRole(
            role, granteePrincipal, expirationEpochTimeSeconds, properties, tableDto);
        break;
      case REVOKE:
        authorizationHandler.revokeRole(role, granteePrincipal, tableDto);
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
   * Creates lock on a table if lock on table is not already set.
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
    TableDto tableDto =
        openHouseInternalRepository
            .findById(TableDtoPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build())
            .orElseThrow(() -> new NoSuchUserTableException(databaseId, tableId));
    authorizationUtils.checkTableLockPrivileges(
        tableDto, tableCreatorUpdater, Privileges.UPDATE_TABLE_METADATA);
    // lock state from incoming request
    LockState lockState =
        LockState.builder()
            .locked(createUpdateLockRequestBody.isLocked())
            .message(createUpdateLockRequestBody.getMessage())
            .expirationInDays(createUpdateLockRequestBody.getExpirationInDays())
            .creationTime(createUpdateLockRequestBody.getCreationTime())
            .build();
    if (createUpdateLockRequestBody.isLocked()) {
      Policies policies = tableDto.getPolicies();
      Policies policiesToSave;
      if (policies != null) {
        policiesToSave = tableDto.getPolicies().toBuilder().lockState(lockState).build();
      } else {
        policiesToSave = Policies.builder().lockState(lockState).build();
      }
      // should allow updating lock on a table with different reason
      TableDto tableDtoToSave =
          tableDto
              .toBuilder()
              .policies(policiesToSave)
              .tableVersion(tableDto.getTableLocation())
              .build();
      saveTableDto(tableDtoToSave, Optional.of(tableDto));
    }
  }

  /**
   * unlock the table by setting the lockState policy to null.
   *
   * @param databaseId
   * @param tableId
   * @param actingPrincipal
   */
  @Override
  public void deleteLock(String databaseId, String tableId, String actingPrincipal) {
    TableDto tableDto =
        openHouseInternalRepository
            .findById(TableDtoPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build())
            .orElseThrow(() -> new NoSuchUserTableException(databaseId, tableId));
    authorizationUtils.checkTableUnLockPrivileges(
        tableDto, actingPrincipal, Privileges.UPDATE_TABLE_METADATA);
    Policies policies = tableDto.getPolicies();
    if (policies != null && policies.getLockState() != null && policies.getLockState().isLocked()) {
      Policies policiesToSave;
      // set lockState policy to null
      policiesToSave = tableDto.getPolicies().toBuilder().lockState(null).build();
      TableDto tableDtoToSave =
          tableDto
              .toBuilder()
              .policies(policiesToSave)
              .tableVersion(tableDto.getTableLocation())
              .build();
      saveTableDto(tableDtoToSave, Optional.of(tableDto));
    }
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
}
