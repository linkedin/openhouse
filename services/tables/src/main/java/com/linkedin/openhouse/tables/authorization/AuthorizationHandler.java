package com.linkedin.openhouse.tables.authorization;

import com.linkedin.openhouse.tables.api.spec.v0.response.components.AclPolicy;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.List;
import java.util.Map;

/**
 * OpenHouse table sharing specification divides table-level access control into two parts:
 *
 * <p>Data ACLs: These are used to control access to the data files that are part of the table.
 * OpenHouse delegates the enforcement of these ACLs to the underlying storage system. -
 *
 * <p>Metadata RBAC: These are used to control access to the metadata of the table, and enforced by
 * Catalog service.
 *
 * <p>For a comprehensive access control solution, implementation of AuthorizationHandler must
 * implement both data acls and metadata rbac.
 */
public interface AuthorizationHandler {

  /**
   * Method to check if principal has privilege on databaseDto.
   *
   * @param principal
   * @param databaseDto
   * @param privilege
   * @return
   */
  boolean checkAccessDecision(String principal, DatabaseDto databaseDto, Privileges privilege);

  /**
   * Method to check if principal has privilege on tableDto.
   *
   * @param principal
   * @param tableDto
   * @param privilege
   * @return
   */
  boolean checkAccessDecision(String principal, TableDto tableDto, Privileges privilege);

  /**
   * Method to assign role to principal on databaseDto, for granting access to a database
   *
   * @param role
   * @param principal User principal to grant the access to.
   * @param databaseDto
   */
  void grantRole(String role, String principal, DatabaseDto databaseDto);

  /**
   * Method to assign role to principal on tableDto, for granting access to a table
   *
   * @param role
   * @param principal User principal to grant the access to.
   * @param expirationEpochTimeSeconds optional epoch time in seconds for the role to expire
   * @param properties Optional properties to accept key-value pair
   * @param tableDto
   */
  void grantRole(
      String role,
      String principal,
      Long expirationEpochTimeSeconds,
      Map<String, String> properties,
      TableDto tableDto);

  /**
   * Method to revoke role from principal on databaseDto, for restricting access to a database
   *
   * @param role
   * @param principal User principal to revoke the access from.
   * @param databaseDto
   */
  void revokeRole(String role, String principal, DatabaseDto databaseDto);

  /**
   * Method to revoke role from principal on tableDto, for restricting access to a table
   *
   * @param role
   * @param principal User principal to revoke the access from.
   * @param tableDto
   */
  void revokeRole(String role, String principal, TableDto tableDto);

  /**
   * Method to list all aclPolicies defined on tableDto
   *
   * @param tableDto
   * @return list of principal, role mappings on tableDto
   */
  List<AclPolicy> listAclPolicies(TableDto tableDto);

  /**
   * Method to list all aclPolicies defined on tableDto for the userPrincipal
   *
   * @param tableDto
   * @return list of role mappings on tableDto for the userPrincipal
   */
  List<AclPolicy> listAclPolicies(TableDto tableDto, String userPrincipal);

  /**
   * Method to list all aclPolicies defined on databaseDto
   *
   * @param databaseDto
   * @return list of principal, role mappings on databaseDto
   */
  List<AclPolicy> listAclPolicies(DatabaseDto databaseDto);
}
