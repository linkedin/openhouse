package com.linkedin.openhouse.tables.mock.authorization;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tables.authorization.Privileges;
import org.junit.jupiter.api.Test;

public class TestPrivileges {
  @Test
  public void validateValidPrivileges() {
    assertTrue(Privileges.Privilege.isSupportedPrivilege("CREATE_TABLE"));
    assertFalse(Privileges.Privilege.isSupportedPrivilege("SELECT_TABLE"));
  }
}
