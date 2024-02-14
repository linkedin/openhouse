package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.components.AclPolicy;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import java.util.List;

/** Service Interface for Implementing /databases endpoint. */
public interface DatabasesService {
  /**
   * Get a list of all {@link DatabaseDto}s in openhouse.
   *
   * @return list of {@link DatabaseDto}
   */
  List<DatabaseDto> getAllDatabases();

  /**
   * Update aclPolicies on a database represented by databaseId if actingPrincipal has the right
   * privilege.
   *
   * @param databaseId
   * @param updateAclPoliciesRequestBody
   * @param actingPrincipal
   */
  void updateDatabaseAclPolicies(
      String databaseId,
      UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody,
      String actingPrincipal);

  /**
   * @param databaseId
   * @param actingPrincipal
   * @return list of aclPolicies on the databaseId
   */
  List<AclPolicy> getDatabaseAclPolicies(String databaseId, String actingPrincipal);
}
