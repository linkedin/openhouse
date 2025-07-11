package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.model.SoftDeletedUserTableKey;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.api.validator.HouseTablesApiValidator;
import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.services.UserTablesService;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class OpenHouseSoftDeletedUserTableHtsApiHandler implements SoftDeletedTablesHtsApiHandler {

  @Autowired private HouseTablesApiValidator<UserTableKey, UserTable> userTablesHtsApiValidator;

  @Autowired private UserTablesService userTableService;

  @Autowired private UserTablesMapper userTablesMapper;

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> getEntity(
      SoftDeletedUserTableKey userTableKey) {
    throw new UnsupportedOperationException("Get Soft deleted user table is unsupported");
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
    userTablesHtsApiValidator.validateGetEntities(userTable);
    return ApiResponse.<GetAllEntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAllEntityResponseBody.<UserTable>builder()
                .pageResults(
                    userTableService
                        .getAllSoftDeletedTables(userTable, page, size, sortBy)
                        .map(userTableDto -> userTablesMapper.toUserTable(userTableDto)))
                .build())
        .build();
  }

  @Override
  public ApiResponse<Void> deleteEntity(
      SoftDeletedUserTableKey softDeletedUserTableKey, boolean isSoftDelete) {
    UserTableKey userTableKey =
        UserTableKey.builder()
            .databaseId(softDeletedUserTableKey.getDatabaseId())
            .tableId(softDeletedUserTableKey.getTableId())
            .build();
    userTablesHtsApiValidator.validateDeleteEntity(userTableKey);
    userTableService.purgeSoftDeletedUserTable(
        softDeletedUserTableKey.getDatabaseId(),
        softDeletedUserTableKey.getTableId(),
        softDeletedUserTableKey.getDeletedAtMs());
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> putEntity(UserTable userTable) {
    throw new UnsupportedOperationException("Put soft deleted user table is unsupported");
  }

  @Override
  public ApiResponse<Void> renameEntity(UserTable fromUserTable, UserTable toUserTable) {
    throw new UnsupportedOperationException("Rename soft deleted user table is unsupported");
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> recoverEntity(
      SoftDeletedUserTableKey softDeletedUserTable) {
    UserTableKey userTableKey =
        UserTableKey.builder()
            .databaseId(softDeletedUserTable.getDatabaseId())
            .tableId(softDeletedUserTable.getTableId())
            .build();
    userTablesHtsApiValidator.validateGetEntity(userTableKey);

    return ApiResponse.<EntityResponseBody<UserTable>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            EntityResponseBody.<UserTable>builder()
                .entity(
                    userTablesMapper.toUserTable(
                        userTableService.recoverUserTable(
                            softDeletedUserTable.getDatabaseId(),
                            softDeletedUserTable.getTableId(),
                            softDeletedUserTable.getDeletedAtMs())))
                .build())
        .build();
  }
}
