package com.linkedin.openhouse.tables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllSoftDeletedTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetTableResponseBody;
import org.springframework.data.domain.Pageable;

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
   * Function to Get one Page of Table Resources in a given databaseId given the page size and sort
   * the results by the sortBy field.
   *
   * @param databaseId
   * @param page
   * @param size
   * @param sortBy
   * @return A page of tables in the given database.
   */
  ApiResponse<GetAllTablesResponseBody> searchTables(
      String databaseId, int page, int size, String sortBy);

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
   * Function to Rename a Table Resource identified by fromTableId in a given fromDatabaseId to
   * toTableId in a given toDatabaseId
   *
   * @param fromDatabaseId
   * @param fromTableId
   * @param toDatabaseId
   * @param toTableId
   * @param actingPrincipal
   * @return empty body on successful delete
   */
  ApiResponse<Void> renameTable(
      String fromDatabaseId,
      String fromTableId,
      String toDatabaseId,
      String toTableId,
      String actingPrincipal);
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
   * @param tableCreatorUpdator
   * @return empty body on successful delete
   */
  ApiResponse<Void> deleteLock(String databaseId, String tableId, String tableCreatorUpdator);

  /**
   * Function to perform a paginated search on soft deleted tables in a given database ID, with
   * optional filtering by table ID
   *
   * @param databaseId The database ID to search in
   * @param tableId The table ID to filter by (optional)
   * @param page The page number
   * @param size The number of results per page
   * @param sortBy The field to sort by (optional)
   * @param actingPrincipal The principal performing the action
   * @return Response containing a page of soft deleted tables
   */
  ApiResponse<GetAllSoftDeletedTablesResponseBody> searchSoftDeletedTables(
      String databaseId, String tableId, int page, int size, String sortBy, String actingPrincipal);

  /**
   * Function to permanently delete a soft deleted table that has passed its purge time
   *
   * @param databaseId The database ID
   * @param tableId The table ID
   * @param purgeAfterMs The timestamp after which tables should be purged (in milliseconds)
   * @param actingPrincipal The principal performing the action
   * @return Empty response on successful purge
   */
  ApiResponse<Void> purgeSoftDeletedTable(
      String databaseId, String tableId, long purgeAfterMs, String actingPrincipal);

  /**
   * Function to restore a soft deleted table
   *
   * @param databaseId The database ID
   * @param tableId The table ID
   * @param deletedAtMs The timestamp when the table was deleted
   * @param actingPrincipal The principal performing the action
   * @return Empty response on successful restore
   */
  ApiResponse<Void> restoreTable(
      String databaseId, String tableId, long deletedAtMs, String actingPrincipal);
}
