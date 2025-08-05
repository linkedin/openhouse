package com.linkedin.openhouse.tables.controller;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.*;

import com.linkedin.openhouse.tables.api.handler.DatabasesApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllDatabasesResponseBody;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * DatabasesController is the controller class for Databases API. This class is responsible for
 * handling all the API requests that are specific to databases. The class uses DatabasesApiHandler
 * to delegate the request to the service layer.
 */
@RestController
public class DatabasesController {
  @Autowired DatabasesApiHandler databasesApiHandler;

  @Operation(
      summary = "List all Databases",
      description = "Returns a list of Database resources.",
      tags = {"Database"})
  @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "Database GET_ALL: OK")})
  @GetMapping(
      value = {"/v0/databases", "/v1/databases"},
      produces = {"application/json"})
  public ResponseEntity<GetAllDatabasesResponseBody> getAllDatabases() {
    com.linkedin.openhouse.common.api.spec.ApiResponse<GetAllDatabasesResponseBody> apiResponse =
        databasesApiHandler.getAllDatabases();

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Paginated list of all Databases",
      description = "Returns a Page of Database resources.",
      tags = {"Database"})
  @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "Database GET_ALL: OK")})
  @GetMapping(
      value = {"/v2/databases"},
      produces = {"application/json"})
  public ResponseEntity<GetAllDatabasesResponseBody> getAllDatabasesPaginated(
      @RequestParam(required = false, defaultValue = "0") int page,
      @RequestParam(required = false, defaultValue = "50") int size,
      @RequestParam(required = false) String sortBy) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<GetAllDatabasesResponseBody> apiResponse =
        databasesApiHandler.getAllDatabases(page, size, sortBy);

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Get AclPolicies on Database",
      description = "Returns principal to role mappings on resource identified by databaseId.",
      tags = {"Database"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "AclPolicies GET: OK"),
        @ApiResponse(responseCode = "400", description = "AclPolicies GET: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "AclPolicies GET: UNAUTHORIZED"),
        @ApiResponse(responseCode = "404", description = "AclPolicies GET: DB_NOT_FOUND")
      })
  @GetMapping(
      value = {
        "/databases/{databaseId}/aclPolicies",
        "/v0/databases/{databaseId}/aclPolicies",
        "/v1/databases/{databaseId}/aclPolicies"
      },
      produces = {"application/json"})
  public ResponseEntity<GetAclPoliciesResponseBody> getDatabaseAclPolicies(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<GetAclPoliciesResponseBody> apiResponse =
        databasesApiHandler.getDatabaseAclPolicies(databaseId, extractAuthenticatedUserPrincipal());
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Update AclPolicies on database",
      description = "Updates role for principal on database identified by databaseId",
      tags = {"Database"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "AclPolicies PATCH: UPDATED"),
        @ApiResponse(responseCode = "400", description = "AclPolicies PATCH: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "AclPolicies PATCH: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "AclPolicies PATCH: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "AclPolicies PATCH: DB_NOT_FOUND")
      })
  @PatchMapping(
      value = {"/v0/databases/{databaseId}/aclPolicies", "/v1/databases/{databaseId}/aclPolicies"},
      produces = {"application/json"})
  public ResponseEntity<Void> updateDatabaseAclPolicies(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(
              description = "Request containing aclPolicies of the Database to be updated",
              required = true,
              schema = @Schema(implementation = UpdateAclPoliciesRequestBody.class))
          @RequestBody
          UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> apiResponse =
        databasesApiHandler.updateDatabaseAclPolicies(
            databaseId, updateAclPoliciesRequestBody, extractAuthenticatedUserPrincipal());
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }
}
