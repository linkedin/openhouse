package com.linkedin.openhouse.tables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllViewsResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetViewResponseBody;

/**
 * Layer between the /v2 views REST routes and the view service. Implementations hold no business
 * logic: they validate, map, delegate, map back and pick a status.
 */
public interface ViewsApiHandler {

  /**
   * Read a single view.
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
   * @param databaseId database identifier
   * @param page zero-based page index
   * @param size page size
   * @param sortBy optional single sort field
   * @param actingPrincipal authenticated user
   * @return 200 with a page of sparse identifier-only view bodies
   */
  ApiResponse<GetAllViewsResponseBody> getAllViews(
      String databaseId, int page, int size, String sortBy, String actingPrincipal);

  /**
   * Create a view.
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
   * @param databaseId database identifier
   * @param viewId view identifier
   * @param actingPrincipal authenticated user
   * @return 204 with no body
   */
  ApiResponse<Void> deleteView(String databaseId, String viewId, String actingPrincipal);
}
