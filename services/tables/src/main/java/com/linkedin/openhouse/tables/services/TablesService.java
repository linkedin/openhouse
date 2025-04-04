package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.components.AclPolicy;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.List;
import org.springframework.data.util.Pair;

/** Service Interface for Implementing /tables endpoint. */
public interface TablesService {

  /**
   * Given a databaseId and tableId, prepare {@link TableDto} if actingPrincipal has the right
   * privilege.
   *
   * @param databaseId
   * @param tableId
   * @param actingPrincipal
   * @return TableDto.
   */
  TableDto getTable(String databaseId, String tableId, String actingPrincipal);

  /**
   * Given a databaseId, prepare list of {@link TableDto}s.
   *
   * @param databaseId
   * @return list of {@link TableDto}
   */
  List<TableDto> searchTables(String databaseId);

  /**
   * Given a {@link CreateUpdateTableRequestBody}, create or update a Openhouse table for it
   *
   * @param createUpdateTableRequestBody
   * @param tableCreatorUpdater authenticated user principal that created a table.
   * @param failOnExist boolean to determine whether to throw an {@link
   *     com.linkedin.openhouse.common.exception.AlreadyExistsException} if table already exists
   * @return A pair of objects: the first {@link TableDto} is the actual saved object, the second
   *     boolean is set to true iff creation occurred. This is to differentiate between creation and
   *     update of {@link TableDto}.
   */
  Pair<TableDto, Boolean> putTable(
      CreateUpdateTableRequestBody createUpdateTableRequestBody,
      String tableCreatorUpdater,
      Boolean failOnExist);

  /**
   * Delete a table represented by databaseId and tableId if actingPrincipal has the right
   * privilege.
   *
   * @param databaseId
   * @param tableId
   * @param actingPrincipal
   */
  void deleteTable(String databaseId, String tableId, String actingPrincipal);

  /**
   * Update aclPolicy on a table represented by databaseId and tableId if actingPrincipal has the
   * right privilege.
   *
   * @param databaseId
   * @param tableId
   * @param updateAclPoliciesRequestBody
   * @param actingPrincipal
   */
  void updateAclPolicies(
      String databaseId,
      String tableId,
      UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody,
      String actingPrincipal);

  /**
   * Given a databaseId and a tableId list all the aclPolicies on the table if actingPrincipal has
   * the right privilege.
   *
   * @param databaseId
   * @param tableId
   * @param actingPrincipal
   * @return list of aclPoilcies on the table
   */
  List<AclPolicy> getAclPolicies(String databaseId, String tableId, String actingPrincipal);

  /**
   * Given a databaseId and tableId list all the aclPolicies on the table for a user principal if
   * actingPrincipal (the caller) has the right privilege.
   *
   * @param databaseId
   * @param tableId
   * @param actingPrincipal
   * @param userPrincipal
   * @return list of aclPolicies on the table
   */
  List<AclPolicy> getAclPolicies(
      String databaseId, String tableId, String actingPrincipal, String userPrincipal);

  /**
   * @param databaseId
   * @param tableId
   * @param createUpdateLockRequestBody
   * @param tableCreatorUpdator
   */
  void createLock(
      String databaseId,
      String tableId,
      CreateUpdateLockRequestBody createUpdateLockRequestBody,
      String tableCreatorUpdator);

  /**
   * Delete a table lock represented by databaseId and tableId if actingPrincipal has the right
   * privilege.
   *
   * @param databaseId
   * @param tableId
   * @param actingPrincipal
   */
  void deleteLock(String databaseId, String tableId, String actingPrincipal);
}
