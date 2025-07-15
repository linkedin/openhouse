package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.components.AclPolicy;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.dto.mapper.DatabasesMapper;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

/** Default Database Service Implementation for /database REST endpoint. */
@Component
public class DatabasesServiceImpl implements DatabasesService {
  @Autowired OpenHouseInternalRepository openHouseInternalRepository;
  @Autowired ClusterProperties clusterProperties;
  @Autowired DatabasesMapper databasesMapper;
  @Autowired AuthorizationHandler authorizationHandler;

  @Override
  public List<DatabaseDto> getAllDatabases() {
    List<TableDtoPrimaryKey> tableIdentifiers = openHouseInternalRepository.findAllIds();
    return tableIdentifiers.stream()
        .map(
            tableDtoPrimaryKey ->
                databasesMapper.toDatabaseDto(
                    tableDtoPrimaryKey.getDatabaseId(), clusterProperties.getClusterName()))
        .distinct()
        .collect(Collectors.toList());
  }

  @Override
  public Page<DatabaseDto> getAllDatabases(Pageable pageable) {
    Page<TableDtoPrimaryKey> tableIdentifiers = openHouseInternalRepository.findAllIds(pageable);
    return tableIdentifiers.map(
        tableDtoPrimaryKey ->
            databasesMapper.toDatabaseDto(
                tableDtoPrimaryKey.getDatabaseId(), clusterProperties.getClusterName()));
  }

  @Override
  public void updateDatabaseAclPolicies(
      String databaseId,
      UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody,
      String actingPrincipal) {

    DatabaseDto databaseDto = DatabaseDto.builder().databaseId(databaseId).build();
    checkDatabasePrivilege(databaseDto, actingPrincipal, Privileges.UPDATE_ACL);

    String role = updateAclPoliciesRequestBody.getRole();
    String granteePrincipal = updateAclPoliciesRequestBody.getPrincipal();

    switch (updateAclPoliciesRequestBody.getOperation()) {
      case GRANT:
        authorizationHandler.grantRole(role, granteePrincipal, databaseDto, actingPrincipal);
        break;
      case REVOKE:
        authorizationHandler.revokeRole(role, granteePrincipal, databaseDto, actingPrincipal);
        break;
      default:
        throw new UnsupportedOperationException("Only GRANT and REVOKE are supported");
    }
  }

  @Override
  public List<AclPolicy> getDatabaseAclPolicies(String databaseId, String actingPrincipal) {
    DatabaseDto databaseDto = DatabaseDto.builder().databaseId(databaseId).build();
    return authorizationHandler.listAclPolicies(databaseDto);
  }

  /** Throws AccessDeniedException if actingPrincipal is not authorized to act on the database. */
  private void checkDatabasePrivilege(
      DatabaseDto databaseDto, String actingPrincipal, Privileges privilege) {
    if (!authorizationHandler.checkAccessDecision(actingPrincipal, databaseDto, privilege)) {
      throw new AccessDeniedException(
          String.format(
              "Operation on database [%s] failed as user [%s] is unauthorized",
              databaseDto.getDatabaseId(), actingPrincipal));
    }
  }
}
