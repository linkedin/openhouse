package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.tables.api.spec.v1.request.DeltaActionsRequestBody;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DeltaActionsServiceImpl implements DeltaActionsService {

  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  @Override
  public TableDto appendDeltaActions(
      String databaseId,
      String tableId,
      DeltaActionsRequestBody deltaActionsRequestBody,
      String tableCreator) {

    Optional<TableDto> tableDto =
        openHouseInternalRepository.findById(
            TableDtoPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build());
    if (!tableDto.isPresent()) {
      throw new NoSuchUserTableException(databaseId, tableId);
    }
    TableDto tableDtoToSave =
        tableDto
            .get()
            .toBuilder()
            .tableVersion(deltaActionsRequestBody.getBaseTableVersion())
            .jsonSnapshots(deltaActionsRequestBody.getJsonActions())
            .build();
    return openHouseInternalRepository.save(tableDtoToSave);
  }
}
