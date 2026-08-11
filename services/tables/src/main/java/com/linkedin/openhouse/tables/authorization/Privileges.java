package com.linkedin.openhouse.tables.authorization;

import java.util.Locale;
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
  SELECT(Privilege.SELECT),
  SELECT_PII(Privilege.SELECT_PII),
  SELECT_HC(Privilege.SELECT_HC);

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

  /**
   * Resolves the column-level read privilege guarding columns tagged with {@code tagName}, e.g.
   * {@code PII} maps to {@link #SELECT_PII}.
   *
   * @param tagName name of a {@link
   *     com.linkedin.openhouse.tables.api.spec.v0.request.components.PolicyTag.Tag}
   * @return the privilege guarding the tag
   */
  public static Privileges forPolicyTag(String tagName) {
    try {
      return Privileges.valueOf("SELECT_" + tagName.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("No column-level privilege is defined for policy tag '%s'", tagName), e);
    }
  }

  public static class Privilege {
    public static final String CREATE_TABLE = "CREATE_TABLE";
    public static final String UPDATE_TABLE_METADATA = "UPDATE_TABLE_METADATA";
    public static final String GET_TABLE_METADATA = "GET_TABLE_METADATA";
    public static final String DELETE_TABLE = "DELETE_TABLE";
    public static final String UPDATE_ACL = "UPDATE_ACL";
    public static final String SYSTEM_ADMIN = "SYSTEM_ADMIN";
    public static final String LOCK_ADMIN = "LOCK_ADMIN";

    public static final String SELECT = "SELECT";

    /**
     * Column-level read privileges. Each one authorizes reading the columns carrying the
     * correspondingly named policy tag, see {@link
     * com.linkedin.openhouse.tables.api.spec.v0.request.components.PolicyTag.Tag}.
     */
    public static final String SELECT_PII = "SELECT_PII";

    public static final String SELECT_HC = "SELECT_HC";

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
