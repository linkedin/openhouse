package com.linkedin.openhouse.tables.api.validator;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;

/**
 * Structural validation for the /v1 views API. No SQL is parsed, translated or validated against an
 * engine here: view SQL stays opaque and semantic rejection belongs to admission.
 *
 * <p>Every method throws {@link
 * com.linkedin.openhouse.tables.exception.ViewRequestValidationFailureException} carrying all
 * accumulated failures joined with {@code "; "}.
 */
public interface ViewsApiValidator {

  /**
   * Validate a request to read a single view.
   *
   * @param databaseId path database identifier
   * @param viewId path view identifier
   */
  void validateGetView(String databaseId, String viewId);

  /**
   * Validate a request to list views in a database.
   *
   * <p>Tokens are opaque; only supplied blank tokens are rejected here.
   *
   * @param databaseId path database identifier
   * @param pageToken opaque continuation token, or null for the first page
   * @param size maximum number of results requested
   * @param sortBy optional single sort field
   */
  void validateGetAllViews(String databaseId, String pageToken, int size, String sortBy);

  /**
   * Validate a POST request to create a view.
   *
   * <p>The body is judged on its own merits. Whether its identifiers agree with the ones in the
   * request path is the controller's rule, applied before this validator runs.
   *
   * @param requestBody the create request
   */
  void validateCreateView(CreateUpdateViewRequestBody requestBody);

  /**
   * Validate a PUT request to replace or create a view.
   *
   * @param requestBody the update request
   */
  void validateUpdateView(CreateUpdateViewRequestBody requestBody);

  /**
   * Validate a request to delete a view.
   *
   * @param databaseId path database identifier
   * @param viewId path view identifier
   */
  void validateDeleteView(String databaseId, String viewId);
}
