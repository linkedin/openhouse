package com.linkedin.openhouse.tables.api.handler.impl;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.handler.IcebergSnapshotsApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.api.validator.IcebergSnapshotsApiValidator;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.services.IcebergSnapshotsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class OpenHouseIcebergSnapshotsApiHandler implements IcebergSnapshotsApiHandler {

  @Autowired private TablesMapper tablesMapper;

  @Autowired private IcebergSnapshotsService icebergSnapshotsService;

  @Autowired private IcebergSnapshotsApiValidator icebergSnapshotsApiValidator;

  @Autowired private ClusterProperties clusterProperties;

  @Override
  public ApiResponse<GetTableResponseBody> putIcebergSnapshots(
      String databaseId,
      String tableId,
      IcebergSnapshotsRequestBody icebergSnapshotRequestBody,
      String tableCreator) {

    icebergSnapshotsApiValidator.validatePutSnapshots(
        clusterProperties.getClusterName(), databaseId, tableId, icebergSnapshotRequestBody);
    Pair<TableDto, Boolean> result =
        icebergSnapshotsService.putIcebergSnapshots(
            databaseId, tableId, icebergSnapshotRequestBody, tableCreator);
    HttpStatus status = result.getSecond() ? HttpStatus.CREATED : HttpStatus.OK;
    return ApiResponse.<GetTableResponseBody>builder()
        .httpStatus(status)
        .responseBody(tablesMapper.toGetTableResponseBody(result.getFirst()))
        .build();
  }
}
