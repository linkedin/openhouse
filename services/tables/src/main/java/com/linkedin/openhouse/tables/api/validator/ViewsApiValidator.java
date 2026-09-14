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
   * <p>The token is opaque: it is checked only for being non-blank when supplied. Its origin,
   * grammar, age and compatibility with this database and sort cannot be checked without the
   * listing implementation that produces it.
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
   * @param clusterId name of the serving cluster
   * @param databaseId path database identifier
   * @param requestBody the create request
   */
  void validateCreateView(
      String clusterId, String databaseId, CreateUpdateViewRequestBody requestBody);

  /**
   * Validate a PUT request to replace or create a view.
   *
   * @param clusterId name of the serving cluster
   * @param databaseId path database identifier
   * @param viewId path view identifier
   * @param requestBody the update request
   */
  void validateUpdateView(
      String clusterId, String databaseId, String viewId, CreateUpdateViewRequestBody requestBody);

  /**
   * Validate a request to delete a view.
   *
   * @param databaseId path database identifier
   * @param viewId path view identifier
   */
  void validateDeleteView(String databaseId, String viewId);
}
