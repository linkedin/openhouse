package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.api.validator.HouseTablesApiValidator;
import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.services.UserTablesService;
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
    userTablesHtsApiValidator.validateGetEntities(userTable);
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
    userTableService.deleteUserTable(userTableKey.getDatabaseId(), userTableKey.getTableId());
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<EntityResponseBody<UserTable>> putEntity(UserTable userTable) {
    userTablesHtsApiValidator.validatePutEntity(userTable);
    Pair<UserTableDto, Boolean> putResult = userTableService.putUserTable(userTable);
    HttpStatus statusCode = putResult.getSecond() ? HttpStatus.OK : HttpStatus.CREATED;
    return ApiResponse.<EntityResponseBody<UserTable>>builder()
        .httpStatus(statusCode)
        .responseBody(
            EntityResponseBody.<UserTable>builder()
                .entity(userTablesMapper.toUserTable(putResult.getFirst()))
                .build())
        .build();
  }
}
