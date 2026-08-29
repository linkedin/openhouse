package com.linkedin.openhouse.housetables.mock;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.handler.UserTableHtsApiHandler;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.model.TestHtsApiConstants;
import lombok.Getter;
import org.springframework.http.HttpStatus;

public class MockUserTableHtsApiHandler implements UserTableHtsApiHandler {

  /**
   * The rename route is table-typed by the handler method the controller calls, so the routing
   * tests need to see what actually reached the handler.
   */
  @Getter private UserTable lastRenameFromTable;

  /** The typed PUT routes normalize before dispatch; these record what actually arrived. */
  @Getter private UserTable lastPutEntity;

  @Getter private UserTable lastPutView;

  @Getter private UserTableKey lastDeletedViewKey;

  @Getter private UserTableKey lastNeutralEntityKey;

  /** The bean is a singleton across a test class, so recorded calls must be cleared per test. */
  public void resetRecordedCalls() {
    this.lastRenameFromTable = null;
    this.lastPutEntity = null;
    this.lastPutView = null;
    this.lastDeletedViewKey = null;
    this.lastNeutralEntityKey = null;
  }

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
  public ApiResponse<Void> deleteEntity(UserTableKey key, boolean isSoftDelete) {
    return null;
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> putEntity(UserTable entity) {
    this.lastPutEntity = entity;
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

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> getNeutralEntity(UserTableKey key) {
    this.lastNeutralEntityKey = key;
    return ApiResponse.<EntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(TestHtsApiConstants.TEST_GET_NEUTRAL_ENTITY_RESPONSE_BODY)
        .build();
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> getViewEntity(UserTableKey key) {
    return ApiResponse.<EntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(TestHtsApiConstants.TEST_GET_USER_VIEW_RESPONSE_BODY)
        .build();
  }

  @Override
  public ApiResponse<GetAllEntityResponseBody<UserTable>> getViewEntities(UserTable userView) {
    return ApiResponse.<GetAllEntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(TestHtsApiConstants.TEST_GET_ALL_USER_VIEWS_RESPONSE_BODY)
        .build();
  }

  @Override
  public ApiResponse<GetAllEntityResponseBody<UserTable>> getViewEntities(
      UserTable userView, int page, int size, String sortBy) {
    return ApiResponse.<GetAllEntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(TestHtsApiConstants.TEST_GET_ALL_USER_VIEWS_RESPONSE_BODY)
        .build();
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> putView(UserTable userView) {
    this.lastPutView = userView;
    return ApiResponse.<EntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(TestHtsApiConstants.TEST_GET_USER_VIEW_RESPONSE_BODY)
        .build();
  }

  @Override
  public ApiResponse<Void> deleteView(UserTableKey key) {
    this.lastDeletedViewKey = key;
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<Void> renameEntity(UserTable fromUserTable, UserTable toUserTable) {
    this.lastRenameFromTable = fromUserTable;
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }
}
