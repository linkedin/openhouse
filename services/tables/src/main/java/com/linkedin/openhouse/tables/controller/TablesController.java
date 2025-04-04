package com.linkedin.openhouse.tables.controller;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.*;

import com.linkedin.openhouse.tables.api.handler.TablesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import com.linkedin.openhouse.tables.authorization.Privileges;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * TablesController is the controller class for the Tables API. This class is responsible for
 * handling all the API requests that are common across different Table formats. The class uses the
 * TablesApiHandler to delegate the request to the service layer.
 */
@RestController
public class TablesController {

  @Autowired private TablesApiHandler tablesApiHandler;

  @Operation(
      summary = "Get Table in a Database",
      description =
          "Returns a Table resource identified by tableId in the database identified by databaseId.",
      tags = {"Table"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Table GET: OK"),
        @ApiResponse(responseCode = "401", description = "Table GET: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "Table GET: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "Table GET: NOT_FOUND")
      })
  @GetMapping(
      value = {
        "/v0/databases/{databaseId}/tables/{tableId}",
        "/v1/databases/{databaseId}/tables/{tableId}"
      },
      produces = {"application/json"})
  @Secured(value = Privileges.Privilege.GET_TABLE_METADATA)
  public ResponseEntity<GetTableResponseBody> getTable(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "Table ID", required = true) @PathVariable String tableId) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetTableResponseBody> apiResponse =
        tablesApiHandler.getTable(databaseId, tableId, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Search Tables in a Database",
      description =
          "Returns a list of Table resources present in a database. Only filter supported is 'database_id'.",
      tags = {"Table"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Table SEARCH: OK"),
        @ApiResponse(responseCode = "400", description = "Table SEARCH: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "Table SEARCH: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "Table SEARCH: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "Table SEARCH: NOT_FOUND")
      })
  @PostMapping(
      value = {
        "/v0/databases/{databaseId}/tables/search",
        "/v1/databases/{databaseId}/tables/search"
      },
      produces = {"application/json"})
  public ResponseEntity<GetAllTablesResponseBody> searchTables(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetAllTablesResponseBody> apiResponse =
        tablesApiHandler.searchTables(databaseId);

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Create a Table",
      description = "Creates and returns a Table resource in a database identified by databaseId",
      tags = {"Table"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "201", description = "Table POST: CREATED"),
        @ApiResponse(responseCode = "400", description = "Table POST: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "Table POST: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "Table POST: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "Table POST: DB_NOT_FOUND"),
        @ApiResponse(responseCode = "409", description = "Table POST: TBL_EXISTS")
      })
  @PostMapping(
      value = {"/v0/databases/{databaseId}/tables", "/v1/databases/{databaseId}/tables"},
      produces = {"application/json"},
      consumes = {"application/json"})
  public ResponseEntity<GetTableResponseBody> createTable(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(
              description = "Request containing details of the Table to be created",
              required = true,
              schema = @Schema(implementation = CreateUpdateTableRequestBody.class))
          @RequestBody
          CreateUpdateTableRequestBody createUpdateTableRequestBody) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetTableResponseBody> apiResponse =
        tablesApiHandler.createTable(
            databaseId, createUpdateTableRequestBody, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Update a Table",
      description =
          "Updates or creates a Table and returns the Table resources. If the table does not exist, it will "
              + "be created. If the table exists, it will be updated.",
      tags = {"Table"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Table PUT: UPDATED"),
        @ApiResponse(responseCode = "201", description = "Table PUT: CREATED"),
        @ApiResponse(responseCode = "400", description = "Table PUT: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "Table PUT: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "Table PUT: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "Table PUT: DB_NOT_FOUND")
      })
  @PutMapping(
      value = {
        "/v0/databases/{databaseId}/tables/{tableId}",
        "/v1/databases/{databaseId}/tables/{tableId}"
      },
      produces = {"application/json"},
      consumes = {"application/json"})
  public ResponseEntity<GetTableResponseBody> updateTable(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "Table ID", required = true) @PathVariable String tableId,
      @Parameter(
              description = "Request containing details of the Table to be created/updated",
              required = true,
              schema = @Schema(implementation = CreateUpdateTableRequestBody.class))
          @RequestBody
          CreateUpdateTableRequestBody createUpdateTableRequestBody) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetTableResponseBody> apiResponse =
        tablesApiHandler.updateTable(
            databaseId, tableId, createUpdateTableRequestBody, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "DELETE Table",
      description = "Deletes a table resource",
      tags = {"Table"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "Table DELETE: NO_CONTENT"),
        @ApiResponse(responseCode = "400", description = "Table DELETE: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "Table DELETE: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "Table DELETE: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "Table DELETE: TBL_DB_NOT_FOUND")
      })
  @DeleteMapping(
      value = {
        "/v0/databases/{databaseId}/tables/{tableId}",
        "/v1/databases/{databaseId}/tables/{tableId}"
      })
  public ResponseEntity<Void> deleteTable(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "Table ID", required = true) @PathVariable String tableId) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> apiResponse =
        tablesApiHandler.deleteTable(databaseId, tableId, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Get AclPolicies for Table",
      description =
          "Returns principal to role mappings on Table resource identified by databaseId and tableId.",
      tags = {"Table"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "AclPolicies GET: OK"),
        @ApiResponse(responseCode = "400", description = "AclPolicies GET: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "AclPolicies GET: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "AclPolicies GET: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "AclPolicies GET: TABLE_NOT_FOUND")
      })
  @GetMapping(
      value = {
        "/v0/databases/{databaseId}/tables/{tableId}/aclPolicies",
        "/v1/databases/{databaseId}/tables/{tableId}/aclPolicies"
      },
      produces = {"application/json"})
  public ResponseEntity<GetAclPoliciesResponseBody> getAclPolicies(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "Table ID", required = true) @PathVariable String tableId) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<? extends GetAclPoliciesResponseBody>
        apiResponse =
            tablesApiHandler.getAclPolicies(
                databaseId, tableId, extractAuthenticatedUserPrincipal());
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Update AclPolicies for Table",
      description = "Updates role for principal on Table identified by databaseId and tableId",
      tags = {"Table"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "AclPolicies PATCH: NO_CONTENT"),
        @ApiResponse(responseCode = "400", description = "AclPolicies PATCH: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "AclPolicies PATCH: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "AclPolicies PATCH: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "AclPolicies PATCH: TABLE_NOT_FOUND")
      })
  @PatchMapping(
      value = {
        "/v0/databases/{databaseId}/tables/{tableId}/aclPolicies",
        "/v1/databases/{databaseId}/tables/{tableId}/aclPolicies"
      },
      produces = {"application/json"})
  public ResponseEntity<Void> updateAclPolicies(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "Table ID", required = true) @PathVariable String tableId,
      @Parameter(
              description = "Request containing aclPolicy of the Table to be created/updated",
              required = true,
              schema = @Schema(implementation = UpdateAclPoliciesRequestBody.class))
          @RequestBody
          UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> apiResponse =
        tablesApiHandler.updateAclPolicies(
            databaseId, tableId, updateAclPoliciesRequestBody, extractAuthenticatedUserPrincipal());
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Get AclPolicies for user principal on a table",
      description =
          "Returns role mappings, access information for a principal on resource identified by databaseId and tableId.",
      tags = {"Table"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "AclPolicies GET: OK"),
        @ApiResponse(responseCode = "400", description = "AclPolicies GET: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "AclPolicies GET: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "AclPolicies GET: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "AclPolicies GET: TABLE_NOT_FOUND")
      })
  @GetMapping(
      value = {
        "/v0/databases/{databaseId}/tables/{tableId}/aclPolicies/{principal}",
        "/v1/databases/{databaseId}/tables/{tableId}/aclPolicies/{principal}"
      },
      produces = {"application/json"})
  public ResponseEntity<GetAclPoliciesResponseBody> getAclPoliciesForUserPrincipal(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "Table ID", required = true) @PathVariable String tableId,
      @Parameter(description = "Principal", required = true) @PathVariable String principal) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<GetAclPoliciesResponseBody> apiResponse =
        tablesApiHandler.getAclPoliciesForUserPrincipal(
            databaseId, tableId, extractAuthenticatedUserPrincipal(), principal);
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Create lock on Table",
      description = "Create lock on a table identified by databaseId and tableId",
      tags = {"Table"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "lock PATCH: NO_CONTENT"),
        @ApiResponse(responseCode = "400", description = "lock PATCH: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "lock PATCH: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "lock PATCH: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "lock PATCH: TABLE_NOT_FOUND")
      })
  @PostMapping(
      value = {"/v1/databases/{databaseId}/tables/{tableId}/lock"},
      produces = {"application/json"})
  public ResponseEntity<Void> createLock(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "Table ID", required = true) @PathVariable String tableId,
      @Parameter(
              description = "Request containing locked state of the Table to be created/updated",
              required = true,
              schema = @Schema(implementation = CreateUpdateLockRequestBody.class))
          @RequestBody
          CreateUpdateLockRequestBody createUpdateLockRequestBody) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> apiResponse =
        tablesApiHandler.createLock(
            databaseId, tableId, createUpdateLockRequestBody, extractAuthenticatedUserPrincipal());
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Delete lock on Table",
      description = "Delete lock on a table identified by databaseId and tableId",
      tags = {"Table"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "lock PATCH: NO_CONTENT"),
        @ApiResponse(responseCode = "400", description = "lock PATCH: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "lock PATCH: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "lock PATCH: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "lock PATCH: TABLE_NOT_FOUND")
      })
  @DeleteMapping(
      value = {"/v1/databases/{databaseId}/tables/{tableId}/lock"},
      produces = {"application/json"})
  public ResponseEntity<Void> deleteLock(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "Table ID", required = true) @PathVariable String tableId) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> apiResponse =
        tablesApiHandler.deleteLock(databaseId, tableId, extractAuthenticatedUserPrincipal());
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }
}
