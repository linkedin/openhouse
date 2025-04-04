package com.linkedin.openhouse.tables.api.handler.impl;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.handler.TablesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.api.validator.TablesApiValidator;
import com.linkedin.openhouse.tables.dto.mapper.TablesMapper;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.services.TablesService;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Default OpenHouse Tables API Handler Implementation that is the layer between REST and Backend.
 */
@Component
public class OpenHouseTablesApiHandler implements TablesApiHandler {

  @Autowired private TablesApiValidator tablesApiValidator;

  @Autowired private TablesService tableService;

  @Autowired private TablesMapper tablesMapper;

  @Autowired private ClusterProperties clusterProperties;

  @Override
  public ApiResponse<GetTableResponseBody> getTable(
      String databaseId, String tableId, String actingPrincipal) {
    tablesApiValidator.validateGetTable(databaseId, tableId);
    return ApiResponse.<GetTableResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            tablesMapper.toGetTableResponseBody(
                tableService.getTable(databaseId, tableId, actingPrincipal)))
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
  public ApiResponse<GetTableResponseBody> createTable(
      String databaseId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody,
      String tableCreator) {
    tablesApiValidator.validateCreateTable(
        clusterProperties.getClusterName(), databaseId, createUpdateTableRequestBody);
    Pair<TableDto, Boolean> putResult =
        tableService.putTable(createUpdateTableRequestBody, tableCreator, true);
    return ApiResponse.<GetTableResponseBody>builder()
        .httpStatus(HttpStatus.CREATED)
        .responseBody(tablesMapper.toGetTableResponseBody(putResult.getFirst()))
        .build();
  }

  @Override
  public ApiResponse<GetTableResponseBody> updateTable(
      String databaseId,
      String tableId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody,
      String tableCreatorUpdator) {
    tablesApiValidator.validateUpdateTable(
        clusterProperties.getClusterName(), databaseId, tableId, createUpdateTableRequestBody);
    Pair<TableDto, Boolean> putResult =
        tableService.putTable(createUpdateTableRequestBody, tableCreatorUpdator, false);
    HttpStatus status = putResult.getSecond() ? HttpStatus.CREATED : HttpStatus.OK;
    return ApiResponse.<GetTableResponseBody>builder()
        .httpStatus(status)
        .responseBody(tablesMapper.toGetTableResponseBody(putResult.getFirst()))
        .build();
  }

  @Override
  public ApiResponse<Void> deleteTable(String databaseId, String tableId, String actingPrincipal) {
    tablesApiValidator.validateDeleteTable(databaseId, tableId);
    tableService.deleteTable(databaseId, tableId, actingPrincipal);
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
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.CREATED).build();
  }
}
