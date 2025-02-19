package com.linkedin.openhouse.tables.authorization;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.tables.api.spec.v0.response.components.AclPolicy;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AuthorizationServiceException;
import org.springframework.stereotype.Component;

/**
 * Reference implementation of AuthorizationHandler based on Open Policy Agent (OPA) implementing
 * metadata RBAC for service. This implementation stores the user-principal, resource and role
 * mapping in OPA. It is expected that a service running OPA is available and configured in the
 * cluster and base URI is provided in the cluster properties. Given this implementation does not
 * provide integration with storage system for data ACLs it should be extended before production
 * use.
 */
@Component
@Slf4j
public class OpaAuthorizationHandler implements AuthorizationHandler {

  @Autowired private ClusterProperties clusterProperties;

  @Autowired(required = false)
  private OpaHandler opaHandler;

  @Override
  public boolean checkAccessDecision(
      String principal, DatabaseDto databaseDto, Privileges privilege) {
    log.info("Checking access for database {} ", databaseDto.getDatabaseId());
    if (clusterProperties.getClusterSecurityTablesAuthorizationOpaBaseUri() == null) {
      log.warn("Opa base uri is not configured. Allowing access by default");
      return true;
    }
    try {
      return opaHandler.checkAccessDecision(principal, databaseDto, privilege);
    } catch (Exception e) {
      throw new AuthorizationServiceException("Cannot perform Opa access check", e);
    }
  }

  @Override
  public boolean checkAccessDecision(String principal, TableDto tableDto, Privileges privilege) {
    log.info(
        "Checking access for database {} table {} ",
        tableDto.getDatabaseId(),
        tableDto.getTableId());
    if (clusterProperties.getClusterSecurityTablesAuthorizationOpaBaseUri() == null) {
      log.warn("Opa base uri is not configured. Allowing access by default");
      return true;
    }
    try {
      return opaHandler.checkAccessDecision(principal, tableDto, privilege);
    } catch (Exception e) {
      throw new AuthorizationServiceException("Cannot perform Opa access check", e);
    }
  }

  @Override
  public void grantRole(String role, String principal, DatabaseDto databaseDto) {
    log.info(
        "Granting role {} to principal {} on database {}",
        role,
        principal,
        databaseDto.getDatabaseId());
    if (clusterProperties.getClusterSecurityTablesAuthorizationOpaBaseUri() == null) {
      log.warn("Opa base uri is not configured. Skipping grant role");
      return;
    }
    try {
      opaHandler.grantRole(role, principal, databaseDto);
    } catch (Exception e) {
      throw new AuthorizationServiceException("Cannot perform Opa grant role", e);
    }
  }

  @Override
  public void grantRole(
      String role,
      String principal,
      Long expirationEpochTimeSeconds,
      Map<String, String> properties,
      TableDto tableDto) {
    log.info(
        "Granting role {} to principal {} on database {} table {}",
        role,
        principal,
        tableDto.getDatabaseId(),
        tableDto.getTableId());
    if (clusterProperties.getClusterSecurityTablesAuthorizationOpaBaseUri() == null) {
      log.warn("Opa base uri is not configured. Skipping grant role");
      return;
    }
    try {
      opaHandler.grantRole(role, principal, tableDto);
    } catch (Exception e) {
      throw new AuthorizationServiceException("Cannot perform Opa grant role", e);
    }
  }

  @Override
  public void revokeRole(String role, String principal, DatabaseDto databaseDto) {
    log.info(
        "Revoking role {} from principal {} on database {}",
        role,
        principal,
        databaseDto.getDatabaseId());
    if (clusterProperties.getClusterSecurityTablesAuthorizationOpaBaseUri() == null) {
      log.warn("Opa base uri is not configured. Skipping revoke role");
      return;
    }
    try {
      opaHandler.revokeRole(role, principal, databaseDto);
    } catch (Exception e) {
      throw new AuthorizationServiceException("Cannot perform Opa revoke role", e);
    }
  }

  @Override
  public void revokeRole(String role, String principal, TableDto tableDto) {
    log.info(
        "Revoking role {} from principal {} on database {} table {}",
        role,
        principal,
        tableDto.getDatabaseId(),
        tableDto.getTableId());
    if (clusterProperties.getClusterSecurityTablesAuthorizationOpaBaseUri() == null) {
      log.warn("Opa base uri is not configured. Skipping revoke role");
      return;
    }
    try {
      opaHandler.revokeRole(role, principal, tableDto);
    } catch (Exception e) {
      throw new AuthorizationServiceException("Cannot perform Opa revoke role", e);
    }
  }

  @Override
  public List<AclPolicy> listAclPolicies(TableDto tableDto) {
    log.info(
        "Listing ACL policies for database {} table {}",
        tableDto.getDatabaseId(),
        tableDto.getTableId());
    if (clusterProperties.getClusterSecurityTablesAuthorizationOpaBaseUri() == null) {
      log.warn("Opa base uri is not configured. Returning empty list");
      return Collections.emptyList();
    }
    try {
      Map<String, List<String>> allRolesOnResource =
          opaHandler.getAllRolesOnResource(tableDto.getDatabaseId(), tableDto.getTableId());

      List<AclPolicy> result =
          allRolesOnResource.entrySet().stream()
              .flatMap(
                  entry ->
                      entry.getValue().stream()
                          .map(
                              role ->
                                  AclPolicy.builder().principal(entry.getKey()).role(role).build()))
              .collect(Collectors.toList());
      return result;
    } catch (Exception e) {
      throw new AuthorizationServiceException("Cannot perform Opa list ACL policies", e);
    }
  }

  @Override
  public List<AclPolicy> listAclPolicies(TableDto tableDto, String userPrincipal) {
    log.info(
        "Listing ACL policies for database {} table {} for principal {}",
        tableDto.getDatabaseId(),
        tableDto.getTableId(),
        userPrincipal);
    if (clusterProperties.getClusterSecurityTablesAuthorizationOpaBaseUri() == null) {
      log.warn("Opa base uri is not configured. Returning empty list");
      return Collections.emptyList();
    }
    try {
      List<String> roles =
          opaHandler.getRolesForPrincipalOnResource(
              tableDto.getDatabaseId(), tableDto.getTableId(), userPrincipal);
      return roles.stream()
          .map(role -> AclPolicy.builder().role(role).principal(userPrincipal).build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new AuthorizationServiceException("Cannot perform Opa list ACL policies", e);
    }
  }

  @Override
  public List<AclPolicy> listAclPolicies(DatabaseDto databaseDto) {
    log.info("Listing ACL policies for database {}", databaseDto.getDatabaseId());
    if (clusterProperties.getClusterSecurityTablesAuthorizationOpaBaseUri() == null) {
      log.warn("Opa base uri is not configured. Returning empty list");
      return Collections.emptyList();
    }
    try {
      Map<String, List<String>> allRolesOnResource =
          opaHandler.getAllRolesOnResource(databaseDto.getDatabaseId());
      return allRolesOnResource.entrySet().stream()
          .flatMap(
              entry ->
                  entry.getValue().stream()
                      .map(
                          role -> AclPolicy.builder().principal(entry.getKey()).role(role).build()))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new AuthorizationServiceException("Cannot perform Opa list ACL policies", e);
    }
  }
}
