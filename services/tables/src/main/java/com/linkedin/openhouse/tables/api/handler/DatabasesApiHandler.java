package com.linkedin.openhouse.tables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAclPoliciesResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetAllDatabasesResponseBody;

public interface DatabasesApiHandler {
  /**
   * Function to Get all Database Resources in openhouse
   *
   * @return Response containing a list of Databases that would be returned to the client.
   */
  ApiResponse<GetAllDatabasesResponseBody> getAllDatabases();

  /**
   * Function to update aclPolicies on a Database Resource identified by databaseId.
   *
   * @param databaseId
   * @param updateAclPoliciesRequestBody
   * @param actingPrincipal
   * @return empty body on successful update
   */
  ApiResponse<Void> updateDatabaseAclPolicies(
      String databaseId,
      UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody,
      String actingPrincipal);

  /**
   * Function to list aclPolicies on a Database Resource identified by databaseId.
   *
   * @param databaseId
   * @param actingPrincipal
   * @return the acl policies defined on the database that would be returned to the client
   */
  ApiResponse<GetAclPoliciesResponseBody> getDatabaseAclPolicies(
      String databaseId, String actingPrincipal);
}
