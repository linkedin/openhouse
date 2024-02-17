package com.linkedin.openhouse.tables.api.validator;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;

public interface TablesApiValidator {

  /**
   * Function to validate a request to get Table Resource for a given databaseId and tableId
   *
   * @param databaseId
   * @param tableId
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if request is
   *     invalid
   */
  void validateGetTable(String databaseId, String tableId);

  /**
   * Function to validate a request to get all Table Resources in a given databaseId
   *
   * @param databaseId
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if request is
   *     invalid
   */
  void validateGetAllTables(String databaseId);

  /**
   * Function to validate a request to get all Table Resources in a given databaseId by filters and
   * requested columns.
   *
   * @param databaseId
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if request is
   *     invalid
   */
  void validateSearchTables(String databaseId);

  /**
   * Function to validate a request to create a Table Resource.
   *
   * @param clusterId
   * @param databaseId
   * @param createUpdateTableRequestBody
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if request is
   *     invalid
   */
  void validateCreateTable(
      String clusterId,
      String databaseId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody);

  /**
   * Function to validate a request to update/create a Table Resource in a given databaseId
   *
   * @param clusterId
   * @param databaseId
   * @param tableId
   * @param createUpdateTableRequestBody
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if request is
   *     invalid
   */
  void validateUpdateTable(
      String clusterId,
      String databaseId,
      String tableId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody);

  /**
   * Function to validate a request to drop a Table Resource with tableId in a given databaseId
   *
   * @param databaseId
   * @param tableId
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if request is
   *     invalid
   */
  void validateDeleteTable(String databaseId, String tableId);

  /**
   * Function to validate a request to update aclPolicy on a Table Resource with tableId in a given
   * databaseId
   *
   * @param databaseId
   * @param tableId
   * @param updateAclPoliciesRequestBody
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if request is
   *     invalid
   */
  void validateUpdateAclPolicies(
      String databaseId, String tableId, UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody);

  /**
   * Function to validate a request to list aclPolicies on a Table resource with tableId in a given
   * databaseId
   *
   * @param databaseId
   * @param tableId
   */
  void validateGetAclPolicies(String databaseId, String tableId);
}
