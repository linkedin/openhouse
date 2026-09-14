package com.linkedin.openhouse.tables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;

/**
 * Layer between the /v1 views REST routes and the view service. Implementations hold no business
 * logic: they validate, map, delegate, map back and pick a status.
 *
 * <p>Authentication and authorization failures surface as HTTP 401 and 403. Operation-specific
 * failure outcomes are listed below; endpoint-wide server and gateway failures are documented by
 * {@link com.linkedin.openhouse.tables.controller.ViewsController}. A write-side 5xx response may
 * leave the commit outcome unknown and must not be retried blindly.
 */
public interface ViewsApiHandler {

  /**
   * Read a single view.
   *
   * <p>Failure outcomes: 400 for invalid identifiers; 404 for a missing database/view or disabled
   * views.
   *
   * @param databaseId database identifier
   * @param viewId view identifier
   * @param actingPrincipal authenticated user
   * @return 200 with the view pointer
   */
  ApiResponse<GetViewResponseBody> getView(
      String databaseId, String viewId, String actingPrincipal);

  /**
   * List views in a database.
   *
   * <p>Failure outcomes: 400 for invalid identifiers, a blank token, a non-positive count or a
   * composite sort; 404 for a missing database or disabled views.
   *
   * <p>The response carries at most {@code size} results plus the service's continuation token. A
   * client continues while that token is present, even when a page is short or empty, and stops
   * when it is absent. Nothing here interprets, derives or validates a token.
   *
   * @param databaseId database identifier
   * @param pageToken opaque continuation token from a previous response, or null for the first page
   * @param size maximum number of results requested
   * @param sortBy optional single sort field
   * @param actingPrincipal authenticated user
   * @return 200 with sparse identifier-only view bodies and an optional continuation token
   */
  ApiResponse<GetAllViewsResponseBody> getAllViews(
      String databaseId, String pageToken, int size, String sortBy, String actingPrincipal);

  /**
   * Create a view.
   *
   * <p>Failure outcomes: 400 for an invalid request; 404 for a missing database or disabled views;
   * 409 for an occupied name. Status 422 is reserved for admission rejection.
   *
   * @param databaseId database identifier
   * @param requestBody the create request
   * @param actingPrincipal authenticated user
   * @return 201 with the created view pointer
   */
  ApiResponse<GetViewResponseBody> createView(
      String databaseId, CreateUpdateViewRequestBody requestBody, String actingPrincipal);

  /**
   * Replace a view, creating it when it does not exist.
   *
   * <p>Failure outcomes: 400 for an invalid request; 404 for a missing database or disabled views;
   * 409 for a name collision or stale base metadata location. Status 422 is reserved for admission
   * rejection.
   *
   * @param databaseId database identifier
   * @param viewId view identifier
   * @param requestBody the update request
   * @param actingPrincipal authenticated user
   * @return 201 when the call created the view, otherwise 200
   */
  ApiResponse<GetViewResponseBody> updateView(
      String databaseId,
      String viewId,
      CreateUpdateViewRequestBody requestBody,
      String actingPrincipal);

  /**
   * Delete a view.
   *
   * <p>Failure outcomes: 400 for invalid identifiers; 404 for a missing database/view or disabled
   * views.
   *
   * @param databaseId database identifier
   * @param viewId view identifier
   * @param actingPrincipal authenticated user
   * @return 204 with no body
   */
  ApiResponse<Void> deleteView(String databaseId, String viewId, String actingPrincipal);
}
