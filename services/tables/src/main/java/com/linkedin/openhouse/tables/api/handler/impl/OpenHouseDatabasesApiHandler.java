package com.linkedin.openhouse.tables.api.handler.impl;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.handler.DatabasesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllDatabasesResponseBody;
import com.linkedin.openhouse.tables.api.validator.DatabasesApiValidator;
import com.linkedin.openhouse.tables.dto.mapper.DatabasesMapper;
import com.linkedin.openhouse.tables.services.DatabasesService;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class OpenHouseDatabasesApiHandler implements DatabasesApiHandler {
  @Autowired private DatabasesService databasesService;

  @Autowired private DatabasesMapper databasesMapper;

  @Autowired private DatabasesApiValidator databasesApiValidator;

  @Override
  public ApiResponse<GetAllDatabasesResponseBody> getAllDatabases() {
    return ApiResponse.<GetAllDatabasesResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAllDatabasesResponseBody.builder()
                .results(
                    databasesService.getAllDatabases().stream()
                        .map(databaseDto -> databasesMapper.toGetDatabaseResponseBody(databaseDto))
                        .collect(Collectors.toList()))
                .build())
        .build();
  }

  @Override
  public ApiResponse<GetAllDatabasesResponseBody> getAllDatabases(
      int page, int size, String sortBy) {
    databasesApiValidator.validateGetAllDatabases(page, size, sortBy);
    return ApiResponse.<GetAllDatabasesResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAllDatabasesResponseBody.builder()
                .pageResults(
                    databasesService
                        .getAllDatabases(page, size, sortBy)
                        .map(databaseDto -> databasesMapper.toGetDatabaseResponseBody(databaseDto)))
                .build())
        .build();
  }

  @Override
  public ApiResponse<GetAclPoliciesResponseBody> getDatabaseAclPolicies(
      String databaseId, String actingPrincipal) {
    databasesApiValidator.validateGetAclPolicies(databaseId);
    return ApiResponse.<GetAclPoliciesResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAclPoliciesResponseBody.builder()
                .results(
                    databasesService.getDatabaseAclPolicies(databaseId, actingPrincipal).stream()
                        .collect(Collectors.toList()))
                .build())
        .build();
  }

  @Override
  public ApiResponse<Void> updateDatabaseAclPolicies(
      String databaseId,
      UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody,
      String actingPrincipal) {
    databasesApiValidator.validateUpdateAclPolicies(databaseId, updateAclPoliciesRequestBody);
    databasesService.updateDatabaseAclPolicies(
        databaseId, updateAclPoliciesRequestBody, actingPrincipal);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }
}
