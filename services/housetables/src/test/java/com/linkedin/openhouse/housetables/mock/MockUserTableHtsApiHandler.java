package com.linkedin.openhouse.housetables.mock;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.handler.UserTableHtsApiHandler;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.model.EntityType;
import com.linkedin.openhouse.housetables.model.TestHtsApiConstants;
import lombok.Getter;
import org.springframework.http.HttpStatus;

public class MockUserTableHtsApiHandler implements UserTableHtsApiHandler {

  /**
   * The rename route owns the type it operates on, so the routing tests need to see what the
   * controller threaded down rather than infer it from a status code.
   */
  @Getter private EntityType lastRenameEntityType;

  /** The typed PUT routes stamp before dispatch; this records what actually arrived. */
  @Getter private UserTable lastPutEntity;

  @Getter private UserTable lastPutView;

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
  public ApiResponse<EntityResponseBody<UserTable>> getNeutralEntity(UserTableKey userTableKey) {
    return ApiResponse.<EntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(TestHtsApiConstants.TEST_GET_NEUTRAL_ENTITY_RESPONSE_BODY)
        .build();
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> getViewEntity(UserTableKey userTableKey) {
    return ApiResponse.<EntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(TestHtsApiConstants.TEST_GET_USER_VIEW_RESPONSE_BODY)
        .build();
  }

  @Override
  public ApiResponse<GetAllEntityResponseBody<UserTable>> getViewEntities(UserTable entity) {
    return ApiResponse.<GetAllEntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(TestHtsApiConstants.TEST_GET_ALL_USER_VIEWS_RESPONSE_BODY)
        .build();
  }

  @Override
  public ApiResponse<GetAllEntityResponseBody<UserTable>> getViewEntities(
      UserTable entity, int page, int size, String sortBy) {
    return ApiResponse.<GetAllEntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(TestHtsApiConstants.TEST_GET_ALL_USER_VIEWS_RESPONSE_BODY)
        .build();
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> putView(UserTable entity) {
    this.lastPutView = entity;
    return ApiResponse.<EntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(TestHtsApiConstants.TEST_GET_USER_VIEW_RESPONSE_BODY)
        .build();
  }

  @Override
  public ApiResponse<Void> deleteView(UserTableKey key) {
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<Void> renameEntity(
      UserTable fromUserTable, UserTable toUserTable, EntityType entityType) {
    this.lastRenameEntityType = entityType;
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }
}
