package com.linkedin.openhouse.tables.authorization;

import com.linkedin.openhouse.tables.api.spec.v0.response.components.AclPolicy;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Dummy Implementation for AuthzHandler. */
@Component
@Slf4j
public class DummyAuthorizationHandler implements AuthorizationHandler {

  @Override
  public boolean checkAccessDecision(
      String principal, DatabaseDto databaseDto, Privileges privilege) {
    return true;
  }

  @Override
  public boolean checkAccessDecision(String principal, TableDto tableDto, Privileges privilege) {
    log.info("Checking access for {}.{} ", tableDto.getDatabaseId(), tableDto.getTableId());
    return true;
  }

  @Override
  public void grantRole(String role, String principal, DatabaseDto databaseDto) {}

  @Override
  public void grantRole(String role, String principal, TableDto tableDto) {}

  @Override
  public void revokeRole(String role, String principal, DatabaseDto databaseDto) {}

  @Override
  public void revokeRole(String role, String principal, TableDto tableDto) {}

  @Override
  public List<AclPolicy> listAclPolicies(TableDto tableDto) {
    return Collections.emptyList();
  }

  @Override
  public List<AclPolicy> listAclPolicies(TableDto tableDto, String userPrincipal) {
    return Collections.emptyList();
  }

  @Override
  public List<AclPolicy> listAclPolicies(DatabaseDto databaseDto) {
    return Collections.emptyList();
  }
}
