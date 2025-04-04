package com.linkedin.openhouse.tables.utils;

import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.TableDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

/** Utility class for authorization related operations. */
@Slf4j
@Component
public class AuthorizationUtils {

  @Autowired AuthorizationHandler authorizationHandler;

  /**
   * * Throws AccessDeniedException if actingPrincipal is not authorized to act on table denoted by
   * tableId.
   *
   * @param tableDto
   * @param actingPrincipal
   * @param privilege
   */
  public void checkTablePrivilege(TableDto tableDto, String actingPrincipal, Privileges privilege) {
    if (!authorizationHandler.checkAccessDecision(actingPrincipal, tableDto, privilege)) {
      throw new AccessDeniedException(
          String.format(
              "Operation on table %s.%s failed as user %s is unauthorized",
              tableDto.getDatabaseId(), tableDto.getTableId(), actingPrincipal));
    }
  }

  /**
   * Checks if actingPrincipal is authorized to do updates on Table.
   *
   * @param tableDto
   * @param actingPrincipal
   * @param privilege
   */
  public void checkTableWritePathPrivileges(
      TableDto tableDto, String actingPrincipal, Privileges privilege) {
    if (tableDto.getTableType().equals(TableType.REPLICA_TABLE)) {
      checkTablePrivilege(tableDto, actingPrincipal, Privileges.SYSTEM_ADMIN);
    } else {
      checkTablePrivilege(tableDto, actingPrincipal, privilege);
    }
  }

  /**
   * Checks if actingPrincipal is authorized to perform lock action on Table.
   *
   * @param tableDto
   * @param actingPrincipal
   * @param privilege
   */
  public void checkTableLockPrivileges(
      TableDto tableDto, String actingPrincipal, Privileges privilege) {
    if (tableDto.getTableType().equals(TableType.REPLICA_TABLE)) {
      String errMsg =
          String.format(
              "Lock Operation on Replica table %s.%s is not permitted.",
              tableDto.getDatabaseId(), tableDto.getTableId());
      throw new UnsupportedOperationException(errMsg);
    } else {
      checkTablePrivilege(tableDto, actingPrincipal, Privileges.LOCK_ADMIN);
    }
  }

  public void checkTableUnLockPrivileges(
      TableDto tableDto, String actingPrincipal, Privileges privilege) {
    checkTablePrivilege(tableDto, actingPrincipal, Privileges.UNLOCK_ADMIN);
  }

  /**
   * Throws AccessDeniedException if actingPrincipal is not authorized to act on database denoted by
   * databaseId.
   *
   * @param databaseId
   * @param actingPrincipal
   * @param privilege
   */
  public void checkDatabasePrivilege(
      String databaseId, String actingPrincipal, Privileges privilege) {
    DatabaseDto databaseDto = DatabaseDto.builder().databaseId(databaseId).build();
    if (!authorizationHandler.checkAccessDecision(actingPrincipal, databaseDto, privilege)) {
      throw new AccessDeniedException(
          String.format(
              "Operation on database [%s] failed as user [%s] is unauthorized",
              databaseDto.getDatabaseId(), actingPrincipal));
    }
  }
}
