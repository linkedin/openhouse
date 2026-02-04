package com.linkedin.openhouse.tables.api.handler.impl;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.handler.IcebergMetadataApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetIcebergMetadataResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetMetadataDiffResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableDataResponseBody;
import com.linkedin.openhouse.tables.dto.mapper.TableMetadataMapper;
import com.linkedin.openhouse.tables.services.IcebergMetadataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * OpenHouse implementation of InternalTablesApiHandler. Delegates to InternalTablesService for
 * business logic.
 */
@Slf4j
@Component
public class OpenHouseIcebergMetadataApiHandler implements IcebergMetadataApiHandler {

  @Autowired private IcebergMetadataService icebergMetadataService;

  @Autowired private TableMetadataMapper tableMetadataMapper;

  @Override
  public ApiResponse<GetIcebergMetadataResponseBody> getIcebergMetadata(
      String databaseId, String tableId, String actingPrincipal) {
    return ApiResponse.<GetIcebergMetadataResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            tableMetadataMapper.toResponseBody(
                icebergMetadataService.getIcebergMetadata(databaseId, tableId, actingPrincipal)))
        .build();
  }

  @Override
  public ApiResponse<GetMetadataDiffResponseBody> getMetadataDiff(
      String databaseId, String tableId, Long snapshotId, String actingPrincipal) {
    return ApiResponse.<GetMetadataDiffResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            tableMetadataMapper.toMetadataDiffResponseBody(
                icebergMetadataService.getMetadataDiff(
                    databaseId, tableId, snapshotId, actingPrincipal)))
        .build();
  }

  @Override
  public ApiResponse<GetTableDataResponseBody> getTableData(
      String databaseId, String tableId, int limit, String actingPrincipal) {
    return ApiResponse.<GetTableDataResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            tableMetadataMapper.toTableDataResponseBody(
                icebergMetadataService.getTableData(databaseId, tableId, limit, actingPrincipal)))
        .build();
  }
}
