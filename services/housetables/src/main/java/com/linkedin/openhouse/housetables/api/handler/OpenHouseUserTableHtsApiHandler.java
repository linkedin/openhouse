package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.NoSuchEntityException;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.api.validator.HouseTablesApiValidator;
import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.services.UserTablesService;
import com.linkedin.openhouse.housetables.services.model.PagedUserViewQuery;
import com.linkedin.openhouse.housetables.services.model.UserViewQuery;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class OpenHouseUserTableHtsApiHandler implements UserTableHtsApiHandler {

  @Autowired private HouseTablesApiValidator<UserTableKey, UserTable> userTablesHtsApiValidator;

  @Autowired private UserTablesService userTableService;

  @Autowired private UserTablesMapper userTablesMapper;

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> getEntity(UserTableKey userTableKey) {
    userTablesHtsApiValidator.validateGetEntity(userTableKey);
    return ApiResponse.<EntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            EntityResponseBody.<UserTable>builder()
                .entity(
                    userTablesMapper.toUserTable(
                        userTableService.getUserTable(
                            userTableKey.getDatabaseId(), userTableKey.getTableId())))
                .build())
        .build();
  }

  @Override
  public ApiResponse<GetAllEntityResponseBody<UserTable>> getEntities(UserTable userTable) {
    userTablesHtsApiValidator.validateGetEntities(userTable);
    return ApiResponse.<GetAllEntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAllEntityResponseBody.<UserTable>builder()
                .results(
                    userTableService.getAllUserTables(userTable).stream()
                        .map(userTableDto -> userTablesMapper.toUserTable(userTableDto))
                        .collect(Collectors.toList()))
                .build())
        .build();
  }

  @Override
  public ApiResponse<GetAllEntityResponseBody<UserTable>> getEntities(
      UserTable userTable, int page, int size, String sortBy) {
    userTablesHtsApiValidator.validateGetEntities(userTable, page, size, sortBy);
    return ApiResponse.<GetAllEntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAllEntityResponseBody.<UserTable>builder()
                .pageResults(
                    userTableService
                        .getAllUserTables(userTable, page, size, sortBy)
                        .map(userTableDto -> userTablesMapper.toUserTable(userTableDto)))
                .build())
        .build();
  }

  @Override
  public ApiResponse<Void> deleteEntity(UserTableKey userTableKey) {
    userTablesHtsApiValidator.validateDeleteEntity(userTableKey);
    userTableService.deleteUserTable(
        userTableKey.getDatabaseId(), userTableKey.getTableId(), false);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<Void> deleteEntity(UserTableKey userTableKey, boolean isSoftDelete) {
    userTablesHtsApiValidator.validateDeleteEntity(userTableKey);
    userTableService.deleteUserTable(
        userTableKey.getDatabaseId(), userTableKey.getTableId(), isSoftDelete);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> putEntity(UserTable userTable) {
    userTablesHtsApiValidator.validatePutEntity(userTable);
    return put(userTableService.putUserTable(userTable));
  }

  /** Both typed writes share one status rule: a first write is 201, an overwrite is 200. */
  private ApiResponse<EntityResponseBody<UserTable>> put(Pair<UserTableDto, Boolean> putResult) {
    HttpStatus statusCode = putResult.getSecond() ? HttpStatus.OK : HttpStatus.CREATED;
    return ApiResponse.<EntityResponseBody<UserTable>>builder()
        .httpStatus(statusCode)
        .responseBody(
            EntityResponseBody.<UserTable>builder()
                .entity(userTablesMapper.toUserTable(putResult.getFirst()))
                .build())
        .build();
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> getNeutralEntity(UserTableKey userTableKey) {
    userTablesHtsApiValidator.validateGetEntity(userTableKey);
    return okEntity(
        userTableService
            .getNeutralEntity(userTableKey.getDatabaseId(), userTableKey.getTableId())
            .orElseThrow(() -> notFound("Entity", userTableKey)));
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> getViewEntity(UserTableKey userTableKey) {
    userTablesHtsApiValidator.validateGetEntity(userTableKey);
    return okEntity(
        userTableService
            .getUserView(userTableKey.getDatabaseId(), userTableKey.getTableId())
            .orElseThrow(() -> notFound("View", userTableKey)));
  }

  @Override
  public ApiResponse<GetAllEntityResponseBody<UserTable>> getViewEntities(UserTable userView) {
    userTablesHtsApiValidator.validateGetEntities(userView);
    return ApiResponse.<GetAllEntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAllEntityResponseBody.<UserTable>builder()
                .results(
                    userTableService.getAllUserViews(toViewQuery(userView)).stream()
                        .map(userTableDto -> userTablesMapper.toUserTable(userTableDto))
                        .collect(Collectors.toList()))
                .build())
        .build();
  }

  @Override
  public ApiResponse<GetAllEntityResponseBody<UserTable>> getViewEntities(
      UserTable userView, int page, int size, String sortBy) {
    userTablesHtsApiValidator.validateGetEntities(userView, page, size, sortBy);
    return ApiResponse.<GetAllEntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAllEntityResponseBody.<UserTable>builder()
                .pageResults(
                    userTableService
                        .getAllUserViews(
                            PagedUserViewQuery.of(toViewQuery(userView), page, size, sortBy))
                        .map(userTableDto -> userTablesMapper.toUserTable(userTableDto)))
                .build())
        .build();
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> putView(UserTable userView) {
    userTablesHtsApiValidator.validatePutEntity(userView);
    return put(userTableService.putUserView(userView));
  }

  @Override
  public ApiResponse<Void> deleteView(UserTableKey userTableKey) {
    userTablesHtsApiValidator.validateDeleteEntity(userTableKey);
    if (!userTableService.deleteUserView(userTableKey.getDatabaseId(), userTableKey.getTableId())) {
      throw notFound("View", userTableKey);
    }
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  /**
   * Query transport stops here. A table pattern with no database to scope it is not constructible,
   * which is why this must run after the validator has accepted the request.
   */
  private static UserViewQuery toViewQuery(UserTable userView) {
    if (userView.getTableId() != null) {
      return UserViewQuery.matchingPattern(userView.getDatabaseId(), userView.getTableId());
    }
    if (userView.getDatabaseId() != null) {
      return UserViewQuery.inDatabase(userView.getDatabaseId());
    }
    return UserViewQuery.all();
  }

  private static NoSuchEntityException notFound(String entityType, UserTableKey key) {
    return new NoSuchEntityException(entityType, key.getDatabaseId() + "." + key.getTableId());
  }

  private ApiResponse<EntityResponseBody<UserTable>> okEntity(UserTableDto userTableDto) {
    return ApiResponse.<EntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            EntityResponseBody.<UserTable>builder()
                .entity(userTablesMapper.toUserTable(userTableDto))
                .build())
        .build();
  }

  @Override
  public ApiResponse<Void> renameEntity(UserTable fromUserTable, UserTable toUserTable) {
    UserTableKey fromUserTableKey =
        UserTableKey.builder()
            .databaseId(fromUserTable.getDatabaseId())
            .tableId(fromUserTable.getTableId())
            .build();
    UserTableKey toUserTableKey =
        UserTableKey.builder()
            .databaseId(toUserTable.getDatabaseId())
            .tableId(toUserTable.getTableId())
            .build();
    userTablesHtsApiValidator.validateRenameEntity(fromUserTableKey, toUserTableKey);
    userTableService.renameUserTable(
        fromUserTable.getDatabaseId(),
        fromUserTable.getTableId(),
        toUserTable.getDatabaseId(),
        toUserTable.getTableId(),
        toUserTable.getMetadataLocation());
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }
}
