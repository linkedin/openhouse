package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import com.linkedin.openhouse.tables.utils.TableUUIDGenerator;
import java.util.Optional;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

@Component
public class IcebergSnapshotsServiceImpl implements IcebergSnapshotsService {

  @Autowired TablesService tablesService;

  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Autowired TablesMapper tablesMapper;

  @Autowired TableUUIDGenerator tableUUIDGenerator;

  @Autowired AuthorizationUtils authorizationUtils;

  @Override
  public Pair<TableDto, Boolean> putIcebergSnapshots(
      String databaseId,
      String tableId,
      IcebergSnapshotsRequestBody icebergSnapshotRequestBody,
      String tableCreatorUpdater) {
    Optional<TableDto> tableDto =
        openHouseInternalRepository.findById(
            TableDtoPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build());

    String clusterId = icebergSnapshotRequestBody.getCreateUpdateTableRequestBody().getClusterId();

    TableDto tableDtoToSave =
        tablesMapper.toTableDto(
            tableDto.orElseGet(
                () ->
                    TableDto.builder()
                        .tableId(tableId)
                        .databaseId(databaseId)
                        .clusterId(clusterId)
                        .tableUri(
                            TableUri.builder()
                                .tableId(tableId)
                                .databaseId(databaseId)
                                .clusterId(clusterId)
                                .build()
                                .toString())
                        .tableUUID(
                            tableUUIDGenerator.generateUUID(icebergSnapshotRequestBody).toString())
                        .tableCreator(tableCreatorUpdater)
                        .build()),
            icebergSnapshotRequestBody);

    if (tableDto.isPresent()) {
      if (isTableLocked(tableDto.get())) {
        authorizationUtils.checkLockTablePrivilege(
            tableDto.get(), tableCreatorUpdater, Privileges.LOCK_WRITER);
      }
      authorizationUtils.checkTableWritePathPrivileges(
          tableDto.get(), tableCreatorUpdater, Privileges.UPDATE_TABLE_METADATA);
    } else {
      authorizationUtils.checkDatabasePrivilege(
          databaseId, tableCreatorUpdater, Privileges.CREATE_TABLE);
    }
    try {
      return Pair.of(openHouseInternalRepository.save(tableDtoToSave), !tableDto.isPresent());
    } catch (BadRequestException e) {
      throw new RequestValidationFailureException(e.getMessage(), e);
    } catch (CommitFailedException ce) {
      throw new EntityConcurrentModificationException(
          TableUri.builder()
              .tableId(tableId)
              .databaseId(databaseId)
              .clusterId(
                  icebergSnapshotRequestBody.getCreateUpdateTableRequestBody().getClusterId())
              .build()
              .toString(),
          String.format(
              "databaseId : %s, tableId : %s, version: %s %s",
              databaseId,
              tableId,
              icebergSnapshotRequestBody.getBaseTableVersion(),
              "The requested table has been modified/created by other processes."),
          ce);
    }
  }

  private boolean isTableLocked(TableDto tableDto) {
    return tableDto.getPolicies() != null
        && tableDto.getPolicies().getLockState() != null
        && tableDto.getPolicies().getLockState().isLocked();
  }
}
