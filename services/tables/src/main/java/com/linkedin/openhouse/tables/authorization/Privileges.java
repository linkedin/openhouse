package com.linkedin.openhouse.tables.authorization;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class to represent different privileges that are required for authorization checks on Tables
 * resources.
 */
public enum Privileges {
  CREATE_TABLE(Privilege.CREATE_TABLE),
  GET_TABLE_METADATA(Privilege.GET_TABLE_METADATA),
  UPDATE_TABLE_METADATA(Privilege.UPDATE_TABLE_METADATA),
  DELETE_TABLE(Privilege.DELETE_TABLE),
  UPDATE_ACL(Privilege.UPDATE_ACL),
  SYSTEM_ADMIN(Privilege.SYSTEM_ADMIN),
  LOCK_ADMIN(Privilege.LOCK_ADMIN),
  UNLOCK_ADMIN(Privilege.UNLOCK_ADMIN),
  SELECT(Privilege.SELECT);

  private String privilege;

  Privileges(String privilege) {
    this.privilege = privilege;
  }

  /**
   * Returns the privilege value
   *
   * @return
   */
  public String getPrivilege() {
    return privilege;
  }

  public static class Privilege {
    public static final String CREATE_TABLE = "CREATE_TABLE";
    public static final String UPDATE_TABLE_METADATA = "UPDATE_TABLE_METADATA";
    public static final String GET_TABLE_METADATA = "GET_TABLE_METADATA";
    public static final String DELETE_TABLE = "DELETE_TABLE";
    public static final String UPDATE_ACL = "UPDATE_ACL";
    public static final String SYSTEM_ADMIN = "SYSTEM_ADMIN";
    public static final String LOCK_ADMIN = "LOCK_ADMIN";
    public static final String UNLOCK_ADMIN = "UNLOCK_ADMIN";

    public static final String SELECT = "SELECT";
    private static final Set<String> SUPPORTED_PRIVILEGES =
        Stream.of(Privileges.values()).map(Privileges::getPrivilege).collect(Collectors.toSet());

    /**
     * Method to check if privilege requested for is one of the supported roles in OH
     *
     * @param privilege
     * @return
     */
    public static boolean isSupportedPrivilege(String privilege) {
      return SUPPORTED_PRIVILEGES.contains(privilege);
    }
  }
}
