package com.linkedin.openhouse.javaclient.mapper;

import com.linkedin.openhouse.javaclient.api.SupportsGrantRevoke;
import com.linkedin.openhouse.tables.client.model.AclPolicy;
import com.linkedin.openhouse.tables.client.model.GetDatabaseResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public final class SparkMapper {
  private SparkMapper() {}

  public static TableIdentifier toTableIdentifier(GetTableResponseBody getTableResponseBody) {
    return TableIdentifier.of(
        getTableResponseBody.getDatabaseId(), getTableResponseBody.getTableId());
  }

  public static Namespace toNamespaces(GetDatabaseResponseBody getDatabaseResponseBody) {
    return Namespace.of(getDatabaseResponseBody.getDatabaseId());
  }

  public static SupportsGrantRevoke.AclPolicyDto toAclPolicyDto(AclPolicy aclPolicy) {
    return new SupportsGrantRevoke.AclPolicyDto(
        Privileges.fromRole(aclPolicy.getRole()).getPrivilege(), aclPolicy.getPrincipal());
  }
}
