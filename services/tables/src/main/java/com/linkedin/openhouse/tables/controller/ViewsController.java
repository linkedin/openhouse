package com.linkedin.openhouse.tables.controller;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.*;

import com.linkedin.openhouse.tables.api.handler.ViewsApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
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
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller for the Views API. Views are a new resource under {@code /v1}, alongside every other
 * OpenHouse resource, and break no existing client: they occupy their own {@code views} path
 * segment, and {@code /v1/databases/{databaseId}/tables/{tableId}} stays table-only.
 *
 * <p>The controller is registered regardless of whether views are enabled, and holds no business
 * logic. Request bodies are deliberately not annotated with {@code @Valid}: the view validator
 * accumulates every structural failure and reports them together, which Spring's fail-fast binding
 * cannot do.
 */
@RestController
public class ViewsController {

  @Autowired private ViewsApiHandler viewsApiHandler;

  @Operation(
      summary = "Get View in a Database",
      description =
          "Returns a View resource identified by viewId in the database identified by databaseId.",
      tags = {"View"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "View GET: OK"),
        @ApiResponse(responseCode = "400", description = "View GET: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "View GET: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "View GET: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "View GET: NOT_FOUND"),
        @ApiResponse(responseCode = "503", description = "View GET: SERVICE_UNAVAILABLE")
      })
  @GetMapping(
      value = {"/v1/databases/{databaseId}/views/{viewId}"},
      produces = {"application/json"})
  @Secured(value = Privileges.Privilege.SELECT)
  public ResponseEntity<GetViewResponseBody> getView(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "View ID", required = true) @PathVariable String viewId) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetViewResponseBody> apiResponse =
        viewsApiHandler.getView(databaseId, viewId, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Search Views in a Database",
      description = "Returns a Page of View resources present in a database.",
      tags = {"View"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "View SEARCH: OK"),
        @ApiResponse(responseCode = "400", description = "View SEARCH: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "View SEARCH: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "View SEARCH: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "View SEARCH: NOT_FOUND"),
        @ApiResponse(responseCode = "503", description = "View SEARCH: SERVICE_UNAVAILABLE")
      })
  @GetMapping(
      value = {"/v1/databases/{databaseId}/views"},
      produces = {"application/json"})
  @Secured(value = Privileges.Privilege.LIST_VIEW)
  public ResponseEntity<GetAllViewsResponseBody> getAllViews(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @RequestParam(required = false, defaultValue = "0") int page,
      @RequestParam(required = false, defaultValue = "50") int size,
      @RequestParam(required = false) String sortBy) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetAllViewsResponseBody> apiResponse =
        viewsApiHandler.getAllViews(
            databaseId, page, size, sortBy, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Create a View",
      description = "Creates and returns a View resource in a database identified by databaseId",
      tags = {"View"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "201", description = "View POST: CREATED"),
        @ApiResponse(responseCode = "400", description = "View POST: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "View POST: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "View POST: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "View POST: DB_NOT_FOUND"),
        @ApiResponse(responseCode = "409", description = "View POST: VIEW_EXISTS"),
        @ApiResponse(responseCode = "422", description = "View POST: UNPROCESSABLE_ENTITY"),
        @ApiResponse(responseCode = "503", description = "View POST: SERVICE_UNAVAILABLE")
      })
  @PostMapping(
      value = {"/v1/databases/{databaseId}/views"},
      produces = {"application/json"},
      consumes = {"application/json"})
  @Secured(value = Privileges.Privilege.CREATE_VIEW)
  public ResponseEntity<GetViewResponseBody> createView(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(
              description = "Request containing details of the View to be created",
              required = true,
              schema = @Schema(implementation = CreateUpdateViewRequestBody.class))
          @RequestBody
          CreateUpdateViewRequestBody createUpdateViewRequestBody) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetViewResponseBody> apiResponse =
        viewsApiHandler.createView(
            databaseId, createUpdateViewRequestBody, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Update a View",
      description =
          "Updates or creates a View and returns the View resource. If the view does not exist, it "
              + "will be created. If the view exists, it will be replaced.",
      tags = {"View"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "View PUT: UPDATED"),
        @ApiResponse(responseCode = "201", description = "View PUT: CREATED"),
        @ApiResponse(responseCode = "400", description = "View PUT: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "View PUT: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "View PUT: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "View PUT: DB_NOT_FOUND"),
        @ApiResponse(responseCode = "409", description = "View PUT: CONFLICT"),
        @ApiResponse(responseCode = "422", description = "View PUT: UNPROCESSABLE_ENTITY"),
        @ApiResponse(responseCode = "503", description = "View PUT: SERVICE_UNAVAILABLE")
      })
  @PutMapping(
      value = {"/v1/databases/{databaseId}/views/{viewId}"},
      produces = {"application/json"},
      consumes = {"application/json"})
  @Secured(value = Privileges.Privilege.UPDATE_VIEW_METADATA)
  public ResponseEntity<GetViewResponseBody> updateView(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "View ID", required = true) @PathVariable String viewId,
      @Parameter(
              description = "Request containing details of the View to be created/updated",
              required = true,
              schema = @Schema(implementation = CreateUpdateViewRequestBody.class))
          @RequestBody
          CreateUpdateViewRequestBody createUpdateViewRequestBody) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetViewResponseBody> apiResponse =
        viewsApiHandler.updateView(
            databaseId, viewId, createUpdateViewRequestBody, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Drop a View",
      description =
          "Drops a View resource identified by viewId in the database identified by databaseId.",
      tags = {"View"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "View DELETE: NO_CONTENT"),
        @ApiResponse(responseCode = "400", description = "View DELETE: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "View DELETE: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "View DELETE: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "View DELETE: VIEW_NOT_FOUND"),
        @ApiResponse(responseCode = "503", description = "View DELETE: SERVICE_UNAVAILABLE")
      })
  @DeleteMapping(
      value = {"/v1/databases/{databaseId}/views/{viewId}"},
      produces = {"application/json"})
  @Secured(value = Privileges.Privilege.DELETE_VIEW)
  public ResponseEntity<Void> deleteView(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(description = "View ID", required = true) @PathVariable String viewId) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> apiResponse =
        viewsApiHandler.deleteView(databaseId, viewId, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }
}
