package com.linkedin.openhouse.tables.controller;

import static com.linkedin.openhouse.common.security.AuthenticationUtils.*;

import com.linkedin.openhouse.tables.api.handler.ViewsApiHandler;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.exception.ViewRequestValidationFailureException;
import com.linkedin.openhouse.tables.exception.ViewValidationErrorCode;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
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
 * Controller for {@code /v1/databases/{databaseId}/views}. Table routes are separate.
 *
 * <p>The controller is registered regardless of whether views are enabled, and holds no business
 * logic. Request bodies are validated by the view validator, which accumulates structural failures
 * before reporting them.
 *
 * <p>Write URL/body identifiers must match before the body is passed to the handler.
 *
 * <p>The endpoint contract includes gateway-generated 502 and 504 responses. These are distinct
 * from service-generated failures and may not carry the service's error body.
 */
@RestController
public class ViewsController {

  private static final String UNSUPPORTED_PAGE_PARAMETER = "page";

  private static final String UNSUPPORTED_PAGE_REJECTION_MESSAGE =
      "page : is not supported; use pageToken for continuation";

  private static final String DATABASE_ID_MISMATCH_MESSAGE_FORMAT =
      "databaseId : provided %s, doesn't match with the RequestBody %s";

  private static final String VIEW_ID_MISMATCH_MESSAGE_FORMAT =
      "viewId : provided %s, doesn't match with the RequestBody %s";

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
        @ApiResponse(responseCode = "500", description = "View GET: Unexpected service failure"),
        @ApiResponse(
            responseCode = "502",
            description = "View GET: Gateway received an invalid upstream response"),
        @ApiResponse(
            responseCode = "503",
            description = "View GET: Service or dependency unavailable"),
        @ApiResponse(
            responseCode = "504",
            description = "View GET: Gateway timed out waiting for upstream")
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
      description =
          "Returns view identifiers and an optional nextPageToken. Continue while nextPageToken"
              + " is present.",
      tags = {"View"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "View SEARCH: OK"),
        @ApiResponse(responseCode = "400", description = "View SEARCH: BAD_REQUEST"),
        @ApiResponse(responseCode = "401", description = "View SEARCH: UNAUTHORIZED"),
        @ApiResponse(responseCode = "403", description = "View SEARCH: FORBIDDEN"),
        @ApiResponse(responseCode = "404", description = "View SEARCH: NOT_FOUND"),
        @ApiResponse(responseCode = "500", description = "View SEARCH: Unexpected service failure"),
        @ApiResponse(
            responseCode = "502",
            description = "View SEARCH: Gateway received an invalid upstream response"),
        @ApiResponse(
            responseCode = "503",
            description = "View SEARCH: Service or dependency unavailable"),
        @ApiResponse(
            responseCode = "504",
            description = "View SEARCH: Gateway timed out waiting for upstream")
      })
  @GetMapping(
      value = {"/v1/databases/{databaseId}/views"},
      produces = {"application/json"})
  @Secured(value = Privileges.Privilege.LIST_VIEW)
  public ResponseEntity<GetAllViewsResponseBody> getAllViews(
      @Parameter(description = "Database ID", required = true) @PathVariable String databaseId,
      @Parameter(
              description =
                  "Opaque token from the previous response's nextPageToken. Omit for the first page.")
          @RequestParam(name = "pageToken", required = false)
          String pageToken,
      @Parameter(description = "Maximum number of views to return")
          @RequestParam(name = "size", required = false, defaultValue = "50")
          int size,
      @Parameter(description = "Optional single field to sort the results by")
          @RequestParam(name = "sortBy", required = false)
          String sortBy,
      HttpServletRequest request) {

    rejectUnsupportedPageParameter(request);

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetAllViewsResponseBody> apiResponse =
        viewsApiHandler.getAllViews(
            databaseId, pageToken, size, sortBy, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  /** Reject page explicitly so Spring cannot silently ignore it and return the first page. */
  private static void rejectUnsupportedPageParameter(HttpServletRequest request) {
    // A bare ?page can have a null value, so check key presence.
    if (request.getParameterMap().containsKey(UNSUPPORTED_PAGE_PARAMETER)) {
      throw new ViewRequestValidationFailureException(
          ViewValidationErrorCode.INVALID_VIEW_DEFINITION, UNSUPPORTED_PAGE_REJECTION_MESSAGE);
    }
  }

  /**
   * Reject identifier mismatches before body validation; leave missing fields to the validator.
   *
   * @param viewId the URL view identifier, or null for POST
   */
  private static void rejectIdentifierMismatch(
      String databaseId, String viewId, CreateUpdateViewRequestBody requestBody) {
    if (requestBody == null) {
      return;
    }

    List<String> mismatches = new ArrayList<>();
    String bodyDatabaseId = requestBody.getDatabaseId();
    if (bodyDatabaseId != null && !bodyDatabaseId.equals(databaseId)) {
      mismatches.add(
          String.format(DATABASE_ID_MISMATCH_MESSAGE_FORMAT, databaseId, bodyDatabaseId));
    }
    String bodyViewId = requestBody.getViewId();
    if (viewId != null && bodyViewId != null && !bodyViewId.equals(viewId)) {
      mismatches.add(String.format(VIEW_ID_MISMATCH_MESSAGE_FORMAT, viewId, bodyViewId));
    }

    if (!mismatches.isEmpty()) {
      throw new ViewRequestValidationFailureException(
          ViewValidationErrorCode.INVALID_VIEW_DEFINITION, mismatches);
    }
  }

  @Operation(
      summary = "Create a View",
      description =
          "Creates and returns a View resource in a database identified by databaseId. A 5xx "
              + "response may leave the commit outcome unknown and must not be retried blindly.",
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
        @ApiResponse(responseCode = "500", description = "View POST: Unexpected service failure"),
        @ApiResponse(
            responseCode = "502",
            description = "View POST: Gateway received an invalid upstream response"),
        @ApiResponse(
            responseCode = "503",
            description = "View POST: Service unavailable or commit outcome unknown"),
        @ApiResponse(
            responseCode = "504",
            description = "View POST: Gateway timed out waiting for upstream")
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

    rejectIdentifierMismatch(databaseId, null, createUpdateViewRequestBody);

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetViewResponseBody> apiResponse =
        viewsApiHandler.createView(
            createUpdateViewRequestBody, extractAuthenticatedUserPrincipal());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Update a View",
      description =
          "Updates or creates a View and returns the View resource. If the view does not exist, it "
              + "will be created. If the view exists, it will be replaced. A 5xx response may leave "
              + "the commit outcome unknown and must not be retried blindly.",
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
        @ApiResponse(responseCode = "500", description = "View PUT: Unexpected service failure"),
        @ApiResponse(
            responseCode = "502",
            description = "View PUT: Gateway received an invalid upstream response"),
        @ApiResponse(
            responseCode = "503",
            description = "View PUT: Service unavailable or commit outcome unknown"),
        @ApiResponse(
            responseCode = "504",
            description = "View PUT: Gateway timed out waiting for upstream")
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

    rejectIdentifierMismatch(databaseId, viewId, createUpdateViewRequestBody);

    com.linkedin.openhouse.common.api.spec.ApiResponse<GetViewResponseBody> apiResponse =
        viewsApiHandler.updateView(
            createUpdateViewRequestBody, extractAuthenticatedUserPrincipal());

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
        @ApiResponse(responseCode = "500", description = "View DELETE: Unexpected service failure"),
        @ApiResponse(
            responseCode = "502",
            description = "View DELETE: Gateway received an invalid upstream response"),
        @ApiResponse(
            responseCode = "503",
            description = "View DELETE: Service or dependency unavailable"),
        @ApiResponse(
            responseCode = "504",
            description = "View DELETE: Gateway timed out waiting for upstream")
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
