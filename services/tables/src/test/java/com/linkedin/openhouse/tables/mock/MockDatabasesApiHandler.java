package com.linkedin.openhouse.tables.mock;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.handler.DatabasesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllDatabasesResponseBody;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
@Primary
public class MockDatabasesApiHandler implements DatabasesApiHandler {

  @Override
  public ApiResponse<GetAllDatabasesResponseBody> getAllDatabases() {
    return ApiResponse.<GetAllDatabasesResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(RequestConstants.TEST_GET_ALL_DATABASES_RESPONSE_BODY)
        .build();
  }

  @Override
  public ApiResponse<GetAllDatabasesResponseBody> getAllDatabases(
      int page, int size, String sortBy) {
    return null;
  }

  @Override
  public ApiResponse<Void> updateDatabaseAclPolicies(
      String databaseId,
      UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody,
      String actingPrincipal) {
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<GetAclPoliciesResponseBody> getDatabaseAclPolicies(
      String databaseId, String actingPrincipal) {
    return ApiResponse.<GetAclPoliciesResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(RequestConstants.TEST_GET_ACL_POLICIES_RESPONSE_BODY)
        .build();
  }
}
