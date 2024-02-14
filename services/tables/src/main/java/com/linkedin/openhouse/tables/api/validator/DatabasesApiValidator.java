package com.linkedin.openhouse.tables.api.validator;

import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;

public interface DatabasesApiValidator {
  /**
   * Function to validate a request to update aclPolicies on a Database Resource with a given
   * databaseId
   *
   * @param databaseId
   * @param updateAclPoliciesRequestBody
   * @throws {@link com.linkedin.openhouse.common.exception.RequestValidationFailureException}
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
}
