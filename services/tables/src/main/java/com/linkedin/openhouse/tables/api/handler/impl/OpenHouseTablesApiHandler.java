package com.linkedin.openhouse.tables.api.handler.impl;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.internal.catalog.repository.StorageLocationRepository;
import com.linkedin.openhouse.tables.api.handler.TablesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateStorageLocationRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllSoftDeletedTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.api.validator.TablesApiValidator;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.services.TablesService;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Default OpenHouse Tables API Handler Implementation that is the layer between REST and Backend.
 */
@Component
@Slf4j
public class OpenHouseTablesApiHandler implements TablesApiHandler {

  @Autowired private TablesApiValidator tablesApiValidator;

  @Autowired private TablesService tableService;

  @Autowired private TablesMapper tablesMapper;

  @Autowired private ClusterProperties clusterProperties;

  @Autowired private StorageLocationRepository storageLocationRepository;

  @Override
  public ApiResponse<GetTableResponseBody> getTable(
      String databaseId, String tableId, String actingPrincipal) {
    tablesApiValidator.validateGetTable(databaseId, tableId);
    TableDto tableDto = tableService.getTable(databaseId, tableId, actingPrincipal);
    java.util.List<com.linkedin.openhouse.internal.catalog.model.StorageLocationDto> locations;
    try {
      locations = storageLocationRepository.getStorageLocationsForTable(tableDto.getTableUUID());
    } catch (Exception e) {
      log.warn(
          "Failed to fetch storage locations for {}.{}: {}", databaseId, tableId, e.getMessage());
      locations = java.util.List.of();
    }
    GetTableResponseBody responseBody =
        tablesMapper
            .toGetTableResponseBody(tableDto)
            .toBuilder()
            .storageLocations(locations)
            .build();
    return ApiResponse.<GetTableResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(responseBody)
        .build();
  }

  @Override
  public ApiResponse<GetAllTablesResponseBody> searchTables(String databaseId) {
    tablesApiValidator.validateSearchTables(databaseId);
    return ApiResponse.<GetAllTablesResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAllTablesResponseBody.builder()
                .results(
                    tableService.searchTables(databaseId).stream()
                        .map(tableDto -> tablesMapper.toGetTableResponseBody(tableDto))
                        .collect(Collectors.toList()))
                .build())
        .build();
  }

  @Override
  public ApiResponse<GetAllTablesResponseBody> searchTables(
      String databaseId, int page, int size, String sortBy) {
    tablesApiValidator.validateSearchTables(databaseId, page, size, sortBy);
    return ApiResponse.<GetAllTablesResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAllTablesResponseBody.builder()
                .pageResults(
                    tableService
                        .searchTables(databaseId, page, size, sortBy)
                        .map(tableDto -> tablesMapper.toGetTableResponseBody(tableDto)))
                .build())
        .build();
  }

  @Override
  public ApiResponse<GetTableResponseBody> createTable(
      String databaseId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody,
      String tableCreator) {
    tablesApiValidator.validateCreateTable(
        clusterProperties.getClusterName(), databaseId, createUpdateTableRequestBody);
    String tableId = createUpdateTableRequestBody.getTableId();

    // Pre-allocate a StorageLocation so its ID can be used in the table path.
    String allocatedSlId = null;
    try {
      com.linkedin.openhouse.internal.catalog.model.StorageLocationDto allocated =
          storageLocationRepository.allocateStorageLocation();
      allocatedSlId = allocated.getStorageLocationId();
      java.util.Map<String, String> props =
          new java.util.HashMap<>(createUpdateTableRequestBody.getTableProperties());
      props.put("openhouse.storageLocationId", allocatedSlId);
      createUpdateTableRequestBody =
          createUpdateTableRequestBody.toBuilder().tableProperties(props).build();
    } catch (Exception e) {
      log.warn(
          "Failed to pre-allocate storage location for {}.{}: {}",
          databaseId,
          tableId,
          e.getMessage());
    }

    Pair<TableDto, Boolean> putResult =
        tableService.putTable(createUpdateTableRequestBody, tableCreator, true);
    TableDto createdTable = putResult.getFirst();

    // After table creation, update the StorageLocation URI and link it to the table.
    java.util.List<com.linkedin.openhouse.internal.catalog.model.StorageLocationDto> locations =
        java.util.List.of();
    if (allocatedSlId != null) {
      try {
        String baseDir = stripMetadataFilename(createdTable.getTableLocation());
        storageLocationRepository.updateStorageLocationUri(allocatedSlId, baseDir);
        storageLocationRepository.addStorageLocationToTable(
            createdTable.getTableUUID(), allocatedSlId);
        locations =
            storageLocationRepository.getStorageLocationsForTable(createdTable.getTableUUID());
      } catch (Exception e) {
        log.warn(
            "Failed to register storage location for {}.{}: {}",
            databaseId,
            tableId,
            e.getMessage());
      }
    }

    GetTableResponseBody responseBody =
        tablesMapper
            .toGetTableResponseBody(createdTable)
            .toBuilder()
            .storageLocations(locations)
            .build();
    return ApiResponse.<GetTableResponseBody>builder()
        .httpStatus(HttpStatus.CREATED)
        .responseBody(responseBody)
        .build();
  }

  /**
   * Strips the metadata JSON filename from a tableLocation URI, returning the base directory. e.g.
   * "hdfs://nn:9000/data/openhouse/db/tbl-uuid/00001-xxx.metadata.json" ->
   * "hdfs://nn:9000/data/openhouse/db/tbl-uuid"
   */
  private static String stripMetadataFilename(String tableLocation) {
    if (tableLocation == null) {
      return "";
    }
    int lastSlash = tableLocation.lastIndexOf('/');
    if (lastSlash > 0 && tableLocation.contains(".metadata.json")) {
      return tableLocation.substring(0, lastSlash);
    }
    return tableLocation;
  }

  @Override
  public ApiResponse<GetTableResponseBody> updateTable(
      String databaseId,
      String tableId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody,
      String tableCreatorUpdator) {
    tablesApiValidator.validateUpdateTable(
        clusterProperties.getClusterName(), databaseId, tableId, createUpdateTableRequestBody);

    // Pre-allocate a StorageLocation so its ID can be used in the table path.
    // This only matters for new tables; for updates the property is ignored by the repo layer.
    String allocatedSlId = null;
    try {
      com.linkedin.openhouse.internal.catalog.model.StorageLocationDto allocated =
          storageLocationRepository.allocateStorageLocation();
      allocatedSlId = allocated.getStorageLocationId();
      java.util.Map<String, String> props =
          new java.util.HashMap<>(createUpdateTableRequestBody.getTableProperties());
      props.put("openhouse.storageLocationId", allocatedSlId);
      createUpdateTableRequestBody =
          createUpdateTableRequestBody.toBuilder().tableProperties(props).build();
    } catch (Exception e) {
      log.warn(
          "Failed to pre-allocate storage location for {}.{}: {}",
          databaseId,
          tableId,
          e.getMessage());
    }

    Pair<TableDto, Boolean> putResult =
        tableService.putTable(createUpdateTableRequestBody, tableCreatorUpdator, false);
    boolean isCreated = putResult.getSecond();
    TableDto tableDto = putResult.getFirst();
    HttpStatus status = isCreated ? HttpStatus.CREATED : HttpStatus.OK;

    // After table creation, update the StorageLocation URI and link it to the table.
    java.util.List<com.linkedin.openhouse.internal.catalog.model.StorageLocationDto> locations =
        java.util.List.of();
    if (isCreated && allocatedSlId != null) {
      try {
        String baseDir = stripMetadataFilename(tableDto.getTableLocation());
        storageLocationRepository.updateStorageLocationUri(allocatedSlId, baseDir);
        storageLocationRepository.addStorageLocationToTable(tableDto.getTableUUID(), allocatedSlId);
        locations = storageLocationRepository.getStorageLocationsForTable(tableDto.getTableUUID());
      } catch (Exception e) {
        log.warn(
            "Failed to register storage location for {}.{}: {}",
            databaseId,
            tableId,
            e.getMessage());
      }
    }

    GetTableResponseBody responseBody =
        tablesMapper
            .toGetTableResponseBody(tableDto)
            .toBuilder()
            .storageLocations(locations)
            .build();
    return ApiResponse.<GetTableResponseBody>builder()
        .httpStatus(status)
        .responseBody(responseBody)
        .build();
  }

  @Override
  public ApiResponse<Void> deleteTable(String databaseId, String tableId, String actingPrincipal) {
    tablesApiValidator.validateDeleteTable(databaseId, tableId);
    tableService.deleteTable(databaseId, tableId, actingPrincipal);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<Void> renameTable(
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String actingPrincipal) {
    tablesApiValidator.validateRenameTable(fromDatabaseId, fromTableId, toDatabaseId, toTableId);
    tableService.renameTable(fromDatabaseId, fromTableId, toDatabaseId, toTableId, actingPrincipal);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<Void> updateAclPolicies(
      String databaseId,
      String tableId,
      UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody,
      String actingPrincipal) {
    tablesApiValidator.validateUpdateAclPolicies(databaseId, tableId, updateAclPoliciesRequestBody);
    tableService.updateAclPolicies(
        databaseId, tableId, updateAclPoliciesRequestBody, actingPrincipal);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<GetAclPoliciesResponseBody> getAclPolicies(
      String databaseId, String tableId, String actingPrincipal) {
    tablesApiValidator.validateGetAclPolicies(databaseId, tableId);
    return ApiResponse.<GetAclPoliciesResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAclPoliciesResponseBody.builder()
                .results(
                    tableService.getAclPolicies(databaseId, tableId, actingPrincipal).stream()
                        .collect(Collectors.toList()))
                .build())
        .build();
  }

  @Override
  public ApiResponse<GetAclPoliciesResponseBody> getAclPoliciesForUserPrincipal(
      String databaseId, String tableId, String actingPrincipal, String userPrincipal) {
    tablesApiValidator.validateGetAclPolicies(databaseId, tableId);
    return ApiResponse.<GetAclPoliciesResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAclPoliciesResponseBody.builder()
                .results(
                    tableService.getAclPolicies(databaseId, tableId, actingPrincipal, userPrincipal)
                        .stream()
                        .collect(Collectors.toList()))
                .build())
        .build();
  }

  @Override
  public ApiResponse<Void> createLock(
      String databaseId,
      String tableId,
      CreateUpdateLockRequestBody createUpdateLockRequestBody,
      String tableCreatorUpdator) {
    tablesApiValidator.validateCreateLock(databaseId, tableId, createUpdateLockRequestBody);
    tableService.createLock(databaseId, tableId, createUpdateLockRequestBody, tableCreatorUpdator);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.CREATED).build();
  }

  @Override
  public ApiResponse<Void> deleteLock(
      String databaseId, String tableId, String tableCreatorUpdator) {
    tablesApiValidator.validateGetTable(databaseId, tableId);
    tableService.deleteLock(databaseId, tableId, tableCreatorUpdator);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<GetAllSoftDeletedTablesResponseBody> searchSoftDeletedTables(
      String databaseId,
      String tableId,
      int page,
      int size,
      String sortBy,
      String actingPrincipal) {
    tablesApiValidator.validateSearchSoftDeletedTables(databaseId, tableId, page, size, sortBy);
    return ApiResponse.<GetAllSoftDeletedTablesResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            tablesMapper.toGetAllSoftDeletedTablesResponseBody(
                tableService.searchSoftDeletedTables(databaseId, tableId, page, size, sortBy)))
        .build();
  }

  @Override
  public ApiResponse<Void> purgeSoftDeletedTable(
      String databaseId, String tableId, long purgeAfterMs, String actingPrincipal) {
    tablesApiValidator.validatePurgeSoftDeletedTable(databaseId, tableId, purgeAfterMs);
    tableService.purgeSoftDeletedTables(databaseId, tableId, purgeAfterMs, actingPrincipal);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<Void> restoreTable(
      String databaseId, String tableId, long deletedAtMs, String actingPrincipal) {
    tablesApiValidator.validateGetTable(databaseId, tableId);
    if (deletedAtMs <= 0) {
      throw new IllegalArgumentException("deletedAtMs must be a positive timestamp");
    }
    tableService.restoreTable(databaseId, tableId, deletedAtMs, actingPrincipal);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<GetTableResponseBody> updateStorageLocation(
      String databaseId,
      String tableId,
      UpdateStorageLocationRequestBody requestBody,
      String actingPrincipal) {
    tablesApiValidator.validateGetTable(databaseId, tableId);
    tableService.updateStorageLocation(
        databaseId, tableId, requestBody.getStorageLocationId(), actingPrincipal);
    // Return the updated table with refreshed storage locations
    TableDto updated = tableService.getTable(databaseId, tableId, actingPrincipal);
    GetTableResponseBody responseBody =
        tablesMapper
            .toGetTableResponseBody(updated)
            .toBuilder()
            .storageLocations(
                storageLocationRepository.getStorageLocationsForTable(updated.getTableUUID()))
            .build();
    return ApiResponse.<GetTableResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(responseBody)
        .build();
  }
}
