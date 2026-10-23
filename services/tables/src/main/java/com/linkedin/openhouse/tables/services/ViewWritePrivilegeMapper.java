package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.authorization.Privileges;
import org.springframework.stereotype.Component;

/**
 * Maps a view write intent to the existing table privilege it is authorized against:
 *
 * <pre>
 * create / absent-PUT-create -> CREATE_TABLE
 * replace                    -> UPDATE_TABLE_METADATA
 * drop                       -> DELETE_TABLE
 * </pre>
 *
 * All three are checked at the database level (checkDatabasePrivilege), reusing existing production
 * role-data. No view write ever sends a {@code *_VIEW} privilege name to OPA.
 */
@Component
public class ViewWritePrivilegeMapper {

  public Privileges forCreate() {
    return Privileges.CREATE_TABLE;
  }

  /**
   * @param viewAlreadyExists whether the prepared capture found an existing view at this key:
   *     {@code false} selects the create privilege (a PUT that creates), {@code true} selects the
   *     replace privilege.
   */
  public Privileges forPut(boolean viewAlreadyExists) {
    return viewAlreadyExists ? Privileges.UPDATE_TABLE_METADATA : Privileges.CREATE_TABLE;
  }

  public Privileges forDelete() {
    return Privileges.DELETE_TABLE;
  }
}
