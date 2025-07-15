package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.components.AclPolicy;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

/** Service Interface for Implementing /databases endpoint. */
public interface DatabasesService {
  /**
   * Get a list of all {@link DatabaseDto}s in openhouse.
   *
   * @return list of {@link DatabaseDto}
   */
  List<DatabaseDto> getAllDatabases();

  /**
   * Get a Page of all {@link DatabaseDto}s in openhouse.
   *
   * @param pageable
   * @return Page of {@link DatabaseDto}
   */
  Page<DatabaseDto> getAllDatabases(Pageable pageable);

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
