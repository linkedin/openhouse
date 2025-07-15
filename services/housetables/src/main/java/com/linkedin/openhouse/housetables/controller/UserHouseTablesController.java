package com.linkedin.openhouse.housetables.controller;

import com.linkedin.openhouse.housetables.api.handler.SoftDeletedTablesHtsApiHandler;
import com.linkedin.openhouse.housetables.api.handler.UserTableHtsApiHandler;
import com.linkedin.openhouse.housetables.api.spec.model.SoftDeletedUserTableKey;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.spec.request.CreateUpdateEntityRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.dto.mapper.UserTablesMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * UserHouseTablesController is the controller class for the /hts/tables endpoint. This class is
 * responsible for handling all the API requests that are specific to user tables. This API is
 * leveraged by Tables Service to persist Table metadata. The class uses UserTableHtsApiHandler to
 * delegate the request to the service layer.
 */
@RestController
public class UserHouseTablesController {
  private static final String HTS_TABLES_GENERAL_ENDPOINT = "/hts/tables";
  private static final String HTS_TABLES_GENERAL_ENDPOINT_V1 = "/v1/hts/tables";
  private static final String HTS_TABLES_QUERY_ENDPOINT = "/hts/tables/query";
  private static final String HTS_TABLES_QUERY_ENDPOINT_V1 = "/v1/hts/tables/query";
  private static final String HTS_TABLES_QUERY_ENDPOINT_SOFT_DELETED =
      "/v1/hts/tables/querySoftDeleted";
  private static final String HTS_TABLES_RENAME_ENDPOINT = "/hts/tables/rename";
  private static final String HTS_TABLES_RESTORE_ENDPOINT = "/hts/tables/restore";
  private static final String HTS_TABLES_PURGE_ENDPOINT = "/hts/tables/purge";

  @Autowired private UserTableHtsApiHandler tableHtsApiHandler;

  @Autowired private SoftDeletedTablesHtsApiHandler softDeletedTablesHtsApiHandler;

  @Autowired private UserTablesMapper userTablesMapper;

  @Operation(
      summary = "Get User Table identified by databaseID and tableId.",
      description = "Returns a User House Table identified by databaseID and tableId.",
      tags = {"UserTable"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "User Table GET: OK"),
        @ApiResponse(responseCode = "404", description = "User Table GET: TBL_DB_NOT_FOUND")
      })
  @GetMapping(
      value = HTS_TABLES_GENERAL_ENDPOINT,
      produces = {"application/json"})
  public ResponseEntity<EntityResponseBody<UserTable>> getUserTable(
      @RequestParam(value = "databaseId") String databaseId,
      @RequestParam(value = "tableId") String tableId) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<EntityResponseBody<UserTable>> apiResponse =
        tableHtsApiHandler.getEntity(
            UserTableKey.builder().databaseId(databaseId).tableId(tableId).build());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Search User Table by filter.",
      description =
          "Returns user table from house table that fulfills the predicate. "
              + "For examples, one could provide {databaseId: d1} in the map to query all tables from database d1.",
      tags = {"UserTable"})
  @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "User Table GET: OK")})
  @GetMapping(
      value = HTS_TABLES_QUERY_ENDPOINT,
      produces = {"application/json"})
  public ResponseEntity<GetAllEntityResponseBody<UserTable>> getUserTables(
      @RequestParam Map<String, String> parameters) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<GetAllEntityResponseBody<UserTable>>
        apiResponse = tableHtsApiHandler.getEntities(userTablesMapper.mapToUserTable(parameters));
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Search User Table by filter.",
      description =
          "Returns paginate user tables from house table that fulfills the predicate. "
              + "For examples, one could provide {databaseId: d1} in the map to query all tables from database d1.",
      tags = {"UserTable"})
  @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "User Table GET: OK")})
  @GetMapping(
      value = HTS_TABLES_QUERY_ENDPOINT_V1,
      produces = {"application/json"})
  public ResponseEntity<GetAllEntityResponseBody<UserTable>> getPaginatedUserTables(
      @RequestParam Map<String, String> parameters,
      @RequestParam(defaultValue = "0") int page,
      @RequestParam(defaultValue = "50") int size,
      @RequestParam(defaultValue = "databaseId") String sortBy) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<GetAllEntityResponseBody<UserTable>>
        apiResponse =
            tableHtsApiHandler.getEntities(
                userTablesMapper.mapToUserTable(parameters), page, size, sortBy);
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Get Soft Deleted User Tables",
      description =
          "Returns paginated soft deleted user tables given databaseId, tableId, and/or purgeAfterMs. "
              + "If purgeAfterMs is provided, it will return all soft deleted tables that expire before the given timestamp",
      tags = {"UserTable"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "User Table GET: OK"),
        @ApiResponse(responseCode = "400", description = "User Table GET: BAD_REQUEST"),
        @ApiResponse(responseCode = "404", description = "User Table GET: TBL_DB_NOT_FOUND")
      })
  @GetMapping(value = HTS_TABLES_QUERY_ENDPOINT_SOFT_DELETED)
  public ResponseEntity<GetAllEntityResponseBody<UserTable>> getSoftDeletedUserTables(
      @RequestParam String databaseId,
      @RequestParam(required = false) String tableId,
      @RequestParam(required = false) Long purgeAfterMs,
      @RequestParam(defaultValue = "0") int page,
      @RequestParam(defaultValue = "50") int size,
      @RequestParam(defaultValue = "databaseId") String sortBy) {
    UserTable searchByTable =
        UserTable.builder()
            .databaseId(databaseId)
            .tableId(tableId)
            .purgeAfterMs(purgeAfterMs)
            .build();
    com.linkedin.openhouse.common.api.spec.ApiResponse<GetAllEntityResponseBody<UserTable>>
        apiResponse = softDeletedTablesHtsApiHandler.getEntities(searchByTable, page, size, sortBy);
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Delete a User Table",
      description = "Delete a User House Table entry identified by databaseID and tableId.",
      tags = {"UserTable"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "User Table DELETE: NO_CONTENT"),
        @ApiResponse(responseCode = "400", description = "User Table DELETE: BAD_REQUEST"),
        @ApiResponse(responseCode = "404", description = "User Table DELETE: TBL_DB_NOT_FOUND")
      })
  @DeleteMapping(value = HTS_TABLES_GENERAL_ENDPOINT)
  public ResponseEntity<Void> deleteTable(
      @RequestParam(value = "databaseId") String databaseId,
      @RequestParam(value = "tableId") String tableId) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> apiResponse;
    apiResponse =
        tableHtsApiHandler.deleteEntity(
            UserTableKey.builder().tableId(tableId).databaseId(databaseId).build(), false);
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Delete a User Table",
      description =
          "Delete a User House Table entry identified by databaseID and tableId. "
              + "Soft delete will store the User House Table entry in a separate table and cleaned up at a later time unless restoreed.",
      tags = {"UserTable"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "User Table DELETE: NO_CONTENT"),
        @ApiResponse(responseCode = "400", description = "User Table DELETE: BAD_REQUEST"),
        @ApiResponse(responseCode = "404", description = "User Table DELETE: TBL_DB_NOT_FOUND")
      })
  @DeleteMapping(value = HTS_TABLES_GENERAL_ENDPOINT_V1)
  public ResponseEntity<Void> deleteTable(
      @RequestParam(value = "databaseId") String databaseId,
      @RequestParam(value = "tableId") String tableId,
      @RequestParam(value = "isSoftDelete") boolean isSoftDelete) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> apiResponse;
    apiResponse =
        tableHtsApiHandler.deleteEntity(
            UserTableKey.builder().tableId(tableId).databaseId(databaseId).build(), isSoftDelete);
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Update a User Table",
      description =
          "Updates or creates a User House Table identified by databaseID and tableId. "
              + "If the table does not exist, it will be created. If the table exists, it will be updated.",
      tags = {"UserTable"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "User Table PUT: UPDATED"),
        @ApiResponse(responseCode = "201", description = "User Table PUT: CREATED"),
        @ApiResponse(responseCode = "400", description = "User Table PUT: BAD_REQUEST"),
        @ApiResponse(responseCode = "404", description = "User Table PUT: DB_NOT_FOUND"),
        @ApiResponse(responseCode = "409", description = "User Table PUT: CONFLICT")
      })
  @PutMapping(
      value = HTS_TABLES_GENERAL_ENDPOINT,
      produces = {"application/json"},
      consumes = {"application/json"})
  public ResponseEntity<EntityResponseBody<UserTable>> putUserTable(
      @Parameter(
              description = "Request containing details of the User Table to be created/updated",
              required = true)
          @RequestBody
          CreateUpdateEntityRequestBody<UserTable> createUpdateTableRequestBody) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<EntityResponseBody<UserTable>> apiResponse =
        tableHtsApiHandler.putEntity(createUpdateTableRequestBody.getEntity());
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Rename a User Table",
      description =
          "Update an existing user table, identified by databaseID and tableId, to a new databaseID and tableId.",
      tags = {"UserTable"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "User Table PATCH: NO_CONTENT"),
        @ApiResponse(responseCode = "400", description = "User Table PATCH: BAD_REQUEST"),
        @ApiResponse(responseCode = "404", description = "User Table PATCH: TBL_DB_NOT_FOUND"),
        @ApiResponse(responseCode = "409", description = "User Table PATCH: CONFLICT")
      })
  @PatchMapping(value = HTS_TABLES_RENAME_ENDPOINT)
  public ResponseEntity<Void> renameTable(
      @RequestParam(value = "fromDatabaseId") String fromDatabaseId,
      @RequestParam(value = "fromTableId") String fromTableId,
      @RequestParam(value = "toDatabaseId") String toDatabaseId,
      @RequestParam(value = "toTableId") String toTableId,
      @RequestParam(value = "metadataLocation") String metadataLocation) {
    UserTable fromUserTable =
        UserTable.builder().databaseId(fromDatabaseId).tableId(fromTableId).build();
    UserTable toUserTable =
        UserTable.builder()
            .databaseId(toDatabaseId)
            .tableId(toTableId)
            .metadataLocation(metadataLocation)
            .build();
    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> apiResponse =
        tableHtsApiHandler.renameEntity(fromUserTable, toUserTable);
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "restore a Soft Deleted User Table",
      description =
          "Restores an existing soft-deleted User House Table identified by databaseID, tableId, and deletedAtMs. "
              + " Will fail if the table's databaseId and tableId is currently in use.",
      tags = {"UserTable"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "User Table PUT: NO_CONTENT"),
        @ApiResponse(responseCode = "400", description = "User Table PUT: BAD_REQUEST"),
        @ApiResponse(responseCode = "404", description = "User Table PUT: TBL_DB_NOT_FOUND"),
        @ApiResponse(responseCode = "409", description = "User Table PUT: CONFLICT")
      })
  @PutMapping(value = HTS_TABLES_RESTORE_ENDPOINT)
  public ResponseEntity<EntityResponseBody<UserTable>> restoreUserTable(
      @RequestParam(value = "databaseId") String databaseId,
      @RequestParam(value = "tableId") String tableId,
      @RequestParam(value = "deletedAtMs") Long deletedAtMs) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<EntityResponseBody<UserTable>> apiResponse;
    apiResponse =
        softDeletedTablesHtsApiHandler.restoreEntity(
            SoftDeletedUserTableKey.builder()
                .databaseId(databaseId)
                .tableId(tableId)
                .deletedAtMs(deletedAtMs)
                .build());
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Purge a Soft Deleted User Table",
      description = "Permanently deletes an existing soft-deleted User Table",
      tags = {"UserTable"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "User Table PUT: NO_CONTENT"),
        @ApiResponse(responseCode = "400", description = "User Table PUT: BAD_REQUEST"),
        @ApiResponse(responseCode = "404", description = "User Table PUT: TBL_DB_NOT_FOUND")
      })
  @DeleteMapping(value = HTS_TABLES_PURGE_ENDPOINT)
  public ResponseEntity<Void> purgeSoftDeletedUserTable(
      @RequestParam(value = "databaseId") String databaseId,
      @RequestParam(value = "tableId") String tableId,
      @RequestParam(value = "deletedAtMs") Long deletedAtMs) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> apiResponse;
    apiResponse =
        softDeletedTablesHtsApiHandler.deleteEntity(
            SoftDeletedUserTableKey.builder()
                .databaseId(databaseId)
                .tableId(tableId)
                .deletedAtMs(deletedAtMs)
                .build());
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }
}
