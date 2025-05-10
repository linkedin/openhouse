package com.linkedin.openhouse.housetables.mock;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.handler.UserTableHtsApiHandler;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.model.TestHtsApiConstants;
import org.springframework.http.HttpStatus;

public class MockUserTableHtsApiHandler implements UserTableHtsApiHandler {
  @Override
  public ApiResponse<GetAllEntityResponseBody<UserTable>> getEntities(UserTable entity) {
    return null;
  }

  @Override
  public ApiResponse<GetAllEntityResponseBody<UserTable>> getEntities(
      UserTable entity, int page, int size, String sortBy) {
    return null;
  }

  @Override
  public ApiResponse<Void> deleteEntity(UserTableKey key) {
    return null;
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> putEntity(UserTable entity) {
    return ApiResponse.<EntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(TestHtsApiConstants.TEST_GET_USER_TABLE_RESPONSE_BODY)
        .build();
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> getEntity(UserTableKey userTable) {
    return ApiResponse.<EntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(TestHtsApiConstants.TEST_GET_USER_TABLE_RESPONSE_BODY)
        .build();
  }
}
