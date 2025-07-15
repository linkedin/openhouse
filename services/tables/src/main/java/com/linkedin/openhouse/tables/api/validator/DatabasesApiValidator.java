package com.linkedin.openhouse.tables.api.validator;

import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;

public interface DatabasesApiValidator {
  /**
   * Function to validate a request to update aclPolicies on a Database Resource with a given
   * databaseId
   *
   * @param databaseId
   * @param updateAclPoliciesRequestBody
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if request is
   *     invalid
   */
  void validateUpdateAclPolicies(
      String databaseId, UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody);

  /**
   * Function to validate a request to get aclPolicies on a Database Resource with a given
   * databaseId
   *
   * @param databaseId
   */
  void validateGetAclPolicies(String databaseId);

  /**
   * Function to validate a request to get all Database Resources with pagination and sorting
   *
   * @param page
   * @param size
   * @param sortBy
   */
  void validateGetAllDatabases(int page, int size, String sortBy);
}
