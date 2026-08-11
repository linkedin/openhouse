package com.linkedin.openhouse.javaclient.mapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PrivilegesTest {

  @Test
  public void testColumnLevelPrivilegesMapToRoles() {
    Assertions.assertEquals(Privileges.SELECT_PII, Privileges.fromPrivilege("SELECT PII"));
    Assertions.assertEquals("PII_VIEWER", Privileges.fromPrivilege("SELECT PII").getRole());
    Assertions.assertEquals(Privileges.SELECT_HC, Privileges.fromPrivilege("SELECT HC"));
    Assertions.assertEquals("HC_VIEWER", Privileges.fromPrivilege("SELECT HC").getRole());
  }

  @Test
  public void testColumnLevelRolesMapBackToPrivileges() {
    Assertions.assertEquals("SELECT PII", Privileges.fromRole("PII_VIEWER").getPrivilege());
    Assertions.assertEquals("SELECT HC", Privileges.fromRole("HC_VIEWER").getPrivilege());
  }

  @Test
  public void testTableLevelPrivilegesAreUnchanged() {
    Assertions.assertEquals("TABLE_VIEWER", Privileges.fromPrivilege("SELECT").getRole());
    Assertions.assertEquals("ACL_EDITOR", Privileges.fromPrivilege("MANAGE GRANTS").getRole());
    Assertions.assertEquals("TABLE_ADMIN", Privileges.fromPrivilege("ALTER").getRole());
    Assertions.assertEquals("TABLE_CREATOR", Privileges.fromPrivilege("CREATE TABLE").getRole());
  }

  /**
   * An unmapped privilege used to surface as a bare NoSuchElementException, which said nothing
   * about what went wrong. "SELECTPII" is what the parser produced before column level grants were
   * fixed, so it is the most likely thing to arrive here.
   */
  @Test
  public void testUnknownPrivilegeIsRejectedWithContext() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> Privileges.fromPrivilege("SELECTPII"));
    Assertions.assertTrue(exception.getMessage().contains("SELECTPII"));
    Assertions.assertTrue(exception.getMessage().contains("SELECT PII"));
  }

  @Test
  public void testUnknownRoleIsRejectedWithContext() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> Privileges.fromRole("NOT_A_ROLE"));
    Assertions.assertTrue(exception.getMessage().contains("NOT_A_ROLE"));
  }
}
