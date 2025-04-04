package com.linkedin.openhouse.tables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;

/**
 * Interface layer between REST and Tables backend. The implementation is injected into the Service
 * Controller.
 */
public interface TablesApiHandler {

  /**
   * Function to Get Table Resource for given databaseId and tableId
   *
   * @param databaseId
   * @param tableId
   * @param actingPrincipal
   * @return the table response body that would be returned to the client.
   */
  ApiResponse<GetTableResponseBody> getTable(
      String databaseId, String tableId, String actingPrincipal);

  /**
   * Function to Get all Table Resources in a given databaseId by filters and return requested
   * columns. If no columns are specified only identifier columns are returned.
   *
   * @param databaseId
   * @return Response containing a list of tables to be returned to the client.
   */
  ApiResponse<GetAllTablesResponseBody> searchTables(String databaseId);

  /**
   * Function to Create Table Resource in a given databaseId
   *
   * @param databaseId
   * @param createUpdateTableRequestBody
   * @return the table response body that would be returned to the client.
   */
  ApiResponse<GetTableResponseBody> createTable(
      String databaseId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody,
      String tableCreator);

  /**
   * Function to Create/Update Table Resource in a given databaseId
   *
   * @param databaseId
   * @param tableId
   * @param createUpdateTableRequestBody
   * @return the table response body that would be returned to the client.
   */
  ApiResponse<GetTableResponseBody> updateTable(
      String databaseId,
      String tableId,
      CreateUpdateTableRequestBody createUpdateTableRequestBody,
      String tableCreatorUpdator);

  /**
   * Function to Delete a Table Resource identified by tableId in a given databaseId
   *
   * @param databaseId
   * @param tableId
   * @param actingPrincipal
   * @return empty body on successful delete
   */
  ApiResponse<Void> deleteTable(String databaseId, String tableId, String actingPrincipal);

  /**
   * Function to update aclPolicy on a Table Resource identified by tableId in a given databaseId.
   *
   * @param databaseId
   * @param tableId
   * @param updateAclPoliciesRequestBody
   * @param actingPrincipal
   * @return empty body on successful update
   */
  ApiResponse<Void> updateAclPolicies(
      String databaseId,
      String tableId,
      UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody,
      String actingPrincipal);

  /**
   * Function to list aclPolicies on a Table Resource identified by tableId in a given databaseId.
   *
   * @param databaseId
   * @param tableId
   * @param actingPrincipal
   * @return the acl policies defined on the table that would be returned to the client
   */
  ApiResponse<GetAclPoliciesResponseBody> getAclPolicies(
      String databaseId, String tableId, String actingPrincipal);

  /**
   * Function to list aclPolicies for a principal on a Table Resource identified by tableId in a
   * given databaseId.
   *
   * @param databaseId
   * @param tableId
   * @param actingPrincipal
   * @param userPrincipal
   * @return the acl policies defined for the userPrincipal on the table that would be returned to
   *     the client
   */
  ApiResponse<GetAclPoliciesResponseBody> getAclPoliciesForUserPrincipal(
      String databaseId, String tableId, String actingPrincipal, String userPrincipal);

  /**
   * @param databaseId
   * @param tableId
   * @param createUpdateLockRequestBody
   * @param tableCreatorUpdator
   * @return empty body on successful update
   */
  ApiResponse<Void> createLock(
      String databaseId,
      String tableId,
      CreateUpdateLockRequestBody createUpdateLockRequestBody,
      String tableCreatorUpdator);

  /**
   * Function to Delete a Table lock Resource identified by tableId in a given databaseId
   *
   * @param databaseId
   * @param tableId
   * @param extractAuthenticatedUserPrincipal
   * @return empty body on successful delete
   */
  ApiResponse<Void> deleteLock(
      String databaseId, String tableId, String extractAuthenticatedUserPrincipal);
}
