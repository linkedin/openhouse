package com.linkedin.openhouse.tables.utils;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.SystemOnlyLockAccessDeniedException;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.authorization.AuthorizationHandler;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.config.TablesMvcConstants;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.TableDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

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
   * * Throws AccessDeniedException if actingPrincipal is not authorized to act on a Locked table
   * denoted by tableId.
   *
   * @param tableDto
   * @param actingPrincipal
   * @param privilege
   */
  public void checkLockTablePrivilege(
      TableDto tableDto, String actingPrincipal, Privileges privilege) {
    if (!authorizationHandler.checkAccessDecision(actingPrincipal, tableDto, privilege)) {
      throw new AccessDeniedException(
          String.format(
              "Operation on table %s.%s failed as user %s is unauthorized to act on Locked table",
              tableDto.getDatabaseId(), tableDto.getTableId(), actingPrincipal));
    }
  }

  /**
   * Throws SystemOnlyLockAccessDeniedException if tableDto has an active SYSTEM_ONLY lock and
   * authorizationHandler denies actingPrincipal. Call it after the privilege check.
   *
   * @param tableDto
   * @param actingPrincipal
   */
  public void checkSystemOnlyLockAccess(TableDto tableDto, String actingPrincipal) {
    LockState lock = tableDto.getPolicies() == null ? null : tableDto.getPolicies().getLockState();
    if (lock == null || !lock.isLocked() || lock.getReason() != LockReason.SYSTEM_ONLY) {
      return;
    }
    if (!authorizationHandler.checkSystemOnlyLockAccess(
        actingPrincipal, tableDto, actionTypeDeclaration())) {
      String message = lock.getMessage();
      String detail = message == null || message.trim().isEmpty() ? "" : ": " + message;
      throw new SystemOnlyLockAccessDeniedException(
          String.format(
              "Table %s.%s has a SYSTEM_ONLY lock%s. Use the reason-targeted OpenHouse unlock endpoint "
                  + "as an authorized lock administrator.",
              tableDto.getDatabaseId(), tableDto.getTableId(), detail));
    }
  }

  /**
   * Returns ACTION_TYPE_SYSTEM for a SYSTEM declaration in any case, or null if there is no
   * declaration or request. Any other supplied value is rejected.
   */
  private static String actionTypeDeclaration() {
    RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
    String declaration =
        attributes instanceof ServletRequestAttributes
            ? ((ServletRequestAttributes) attributes)
                .getRequest()
                .getHeader(TablesMvcConstants.HTTP_HEADER_ACTION_TYPE)
            : null;
    if (declaration == null) {
      return null;
    }
    if (TablesMvcConstants.ACTION_TYPE_SYSTEM.equalsIgnoreCase(declaration)) {
      return TablesMvcConstants.ACTION_TYPE_SYSTEM;
    }
    throw new RequestValidationFailureException(
        TablesMvcConstants.HTTP_HEADER_ACTION_TYPE
            + " must be "
            + TablesMvcConstants.ACTION_TYPE_SYSTEM
            + " when supplied.");
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
   * Checks if actingPrincipal is authorized to drop a table. Unlike checkTableWritePathPrivileges,
   * this method does not enforce SYSTEM_ADMIN privilege for REPLICA tables.
   *
   * @param tableDto
   * @param actingPrincipal
   * @param privilege
   */
  public void checkTableDropPrivilege(
      TableDto tableDto, String actingPrincipal, Privileges privilege) {
    checkTablePrivilege(tableDto, actingPrincipal, privilege);
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
