package com.linkedin.openhouse.tables.authorization;

import com.linkedin.openhouse.tables.api.spec.v0.response.components.AclPolicy;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.List;

/** Interface for implementing custom authorization for table-service. */
public interface AuthorizationHandler {

  /**
   * Method to check if {@param principal} has {@param privilege} on {@param databaseDto}.
   *
   * @param principal
   * @param databaseDto
   * @param privilege
   * @return
   */
  boolean checkAccessDecision(String principal, DatabaseDto databaseDto, Privileges privilege);

  /**
   * Method to check if {@param principal} has {@param privilege} on {@param tableDto}.
   *
   * @param principal
   * @param tableDto
   * @param privilege
   * @return
   */
  boolean checkAccessDecision(String principal, TableDto tableDto, Privileges privilege);

  /**
   * Method to assign {@param role} to {@param principal} on {@param databaseDto}, for granting
   * access to a database
   *
   * @param role
   * @param principal User principal to grant the access to.
   * @param databaseDto
   */
  void grantRole(String role, String principal, DatabaseDto databaseDto);

  /**
   * Method to assign {@param role} to {@param principal} on {@param tableDto}, for granting access
   * to a table
   *
   * @param role
   * @param principal User principal to grant the access to.
   * @param tableDto
   */
  void grantRole(String role, String principal, TableDto tableDto);

  /**
   * Method to revoke {@param role} from {@param principal} on {@param databaseDto}, for restricting
   * access to a database
   *
   * @param role
   * @param principal User principal to revoke the access from.
   * @param databaseDto
   */
  void revokeRole(String role, String principal, DatabaseDto databaseDto);

  /**
   * Method to revoke {@param role} from {@param principal} on {@param tableDto}, for restricting
   * access to a table
   *
   * @param role
   * @param principal User principal to revoke the access from.
   * @param tableDto
   */
  void revokeRole(String role, String principal, TableDto tableDto);

  /**
   * Method to list all aclPolicies defined on {@param tableDto}
   *
   * @param tableDto
   * @return list of principal, role mappings on tableDto
   */
  List<AclPolicy> listAclPolicies(TableDto tableDto);

  /**
   * Method to list all aclPolicies defined on {@param tableDto} for the userPrincipal
   *
   * @param tableDto
   * @return list of role mappings on tableDto for the userPrincipal
   */
  List<AclPolicy> listAclPolicies(TableDto tableDto, String userPrincipal);

  /**
   * Method to list all aclPolicies defined on {@param databaseDto}
   *
   * @param databaseDto
   * @return list of principal, role mappings on databaseDto
   */
  List<AclPolicy> listAclPolicies(DatabaseDto databaseDto);
}
