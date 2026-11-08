package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.linkedin.openhouse.tables.authorization.Privileges;
import org.junit.jupiter.api.Test;

public class ViewWritePrivilegeMapperTest {

  private final ViewWritePrivilegeMapper mapper = new ViewWritePrivilegeMapper();

  @Test
  public void createAndAbsentPutUseExistingCreateTablePrivilege() {
    assertEquals(Privileges.CREATE_TABLE, mapper.forCreate());
    assertEquals(Privileges.CREATE_TABLE, mapper.forPut(/* viewAlreadyExists= */ false));
  }

  @Test
  public void replaceUsesExistingUpdateTableMetadataPrivilege() {
    assertEquals(Privileges.UPDATE_TABLE_METADATA, mapper.forPut(/* viewAlreadyExists= */ true));
  }

  @Test
  public void deleteUsesExistingDeleteTablePrivilege() {
    assertEquals(Privileges.DELETE_TABLE, mapper.forDelete());
  }
}
