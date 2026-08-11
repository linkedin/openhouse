package com.linkedin.openhouse.javaclient.mapper;

import java.util.Arrays;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum Privileges {
  SELECT("SELECT", "TABLE_VIEWER"),
  DESCRIBE("DESCRIBE", "TABLE_VIEWER"),
  GRANT_REVOKE("MANAGE GRANTS", "ACL_EDITOR"),
  ALTER("ALTER", "TABLE_ADMIN"),
  CREATE_TABLE("CREATE TABLE", "TABLE_CREATOR"),
  SELECT_PII("SELECT PII", "PII_VIEWER"),
  SELECT_HC("SELECT HC", "HC_VIEWER");

  private final String privilege;
  private final String role;

  public static Privileges fromPrivilege(String privilegeString) {
    return Arrays.stream(Privileges.values())
        .filter(x -> x.getPrivilege().equals(privilegeString))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "Unsupported privilege '%s', expected one of %s",
                        privilegeString, supportedPrivileges())));
  }

  public static Privileges fromRole(String roleString) {
    return Arrays.stream(Privileges.values())
        .filter(x -> x.getRole().equals(roleString))
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException(String.format("Unsupported role '%s'", roleString)));
  }

  private static String supportedPrivileges() {
    return Arrays.stream(Privileges.values())
        .map(Privileges::getPrivilege)
        .distinct()
        .collect(Collectors.joining(", "));
  }
}
