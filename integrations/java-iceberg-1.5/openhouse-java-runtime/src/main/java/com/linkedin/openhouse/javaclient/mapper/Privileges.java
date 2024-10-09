package com.linkedin.openhouse.javaclient.mapper;

import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum Privileges {
  SELECT("SELECT", "TABLE_VIEWER"),
  DESCRIBE("DESCRIBE", "TABLE_VIEWER"),
  GRANT_REVOKE("MANAGE GRANTS", "ACL_EDITOR"),
  ALTER("ALTER", "TABLE_ADMIN"),
  CREATE_TABLE("CREATE TABLE", "TABLE_CREATOR");

  private final String privilege;
  private final String role;

  public static Privileges fromPrivilege(String privilegeString) {
    return Arrays.stream(Privileges.values())
        .filter(x -> x.getPrivilege().equals(privilegeString))
        .findFirst()
        .get();
  }

  public static Privileges fromRole(String roleString) {
    return Arrays.stream(Privileges.values())
        .filter(x -> x.getRole().equals(roleString))
        .findFirst()
        .get();
  }
}
