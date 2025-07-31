package com.linkedin.openhouse.tables.api.validator;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import org.springframework.data.domain.Pageable;

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
   * Function to validate a request to get all Table Resources in a given databaseId by filters and
   * requested columns.
   *
   * @param databaseId
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if request is
   *     invalid
   */
  void validateSearchTables(String databaseId);

  /**
   * Function to validate a request to get a Page of Table Resources in a given databaseId.
   *
   * @param databaseId
   * @param page
   * @param size
   * @param sortBy
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if request is
   *     invalid
   */
  void validateSearchTables(String databaseId, int page, int size, String sortBy);

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
   * Function to validate a request to rename a Table Resource
   *
   * @param fromDatabaseId
   * @param fromTableId
   * @param toDatabaseId
   * @param toTableId
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if request is
   *     invalid
   */
  void validateRenameTable(
      String fromDatabaseId, String fromTableId, String toDatabaseId, String toTableId);

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

  /**
   * @param databaseId
   * @param tableId
   * @param createUpdateLockRequestBody
   */
  void validateCreateLock(
      String databaseId, String tableId, CreateUpdateLockRequestBody createUpdateLockRequestBody);

  /**
   * Function to validate a request to search soft deleted tables in a given databaseId
   *
   * @param databaseId The database ID to search in
   * @param pageable Pagination parameters
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if request is
   *     invalid
   */
  void validateSearchSoftDeletedTables(String databaseId, Pageable pageable);

  /**
   * Function to validate a request to purge a soft deleted table
   *
   * @param databaseId The database ID
   * @param tableId The table ID
   * @param purgeAfterMs The timestamp after which tables should be purged (in milliseconds)
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if request is
   *     invalid
   */
  void validatePurgeSoftDeletedTable(String databaseId, String tableId, long purgeAfterMs);
}
