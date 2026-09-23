package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.SystemOnlyLockAccessDeniedException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.common.utils.ActionTypeContext;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.model.TableDto;

/** Enforces data-access lock rules and protects lock metadata outside the lifecycle API. */
final class LockPolicyValidator {
  private LockPolicyValidator() {}

  /** Evaluate only after the caller's data-access authorization succeeds. */
  static void checkSystemOnlyAccess(TableDto table) {
    LockState lock = lockState(table);
    if (isActiveSystemOnly(lock) && !ActionTypeContext.isSystemAction()) {
      String message = lock.getMessage();
      String detail = message == null || message.trim().isEmpty() ? "" : ": " + message;
      throw new SystemOnlyLockAccessDeniedException(
          String.format(
              "Table %s.%s has a SYSTEM_ONLY lock%s. Use the reason-targeted OpenHouse unlock endpoint "
                  + "as an authorized lock administrator.",
              table.getDatabaseId(), table.getTableId(), detail));
    }
  }

  /** Apply legacy or SYSTEM_ONLY write restrictions after the caller's authorization succeeds. */
  static void checkWrite(TableDto table) {
    LockState lock = lockState(table);
    if (lock == null || !lock.isLocked()) {
      return;
    }
    if (isActiveSystemOnly(lock)) {
      checkSystemOnlyAccess(table);
    } else {
      throw new UnsupportedClientOperationException(
          UnsupportedClientOperationException.Operation.LOCKED_TABLE_OPERATION,
          String.format(
              "Table %s.%s is in locked state and cannot be written to",
              table.getDatabaseId(), table.getTableId()));
    }
  }

  /** Preserve active SYSTEM_ONLY locks and reject changes outside the lock lifecycle API. */
  static TableDto prepare(TableDto current, TableDto mapped) {
    LockState existing = lockState(current);
    LockState requested = lockState(mapped);
    boolean existingSystemOnly = isActiveSystemOnly(existing);
    if ((existingSystemOnly || isActiveSystemOnly(requested))
        && requested != null
        && !requested.equals(existing)) {
      throw new RequestValidationFailureException(
          "SYSTEM_ONLY lock state can only be changed through the lock lifecycle API.");
    }
    if (existingSystemOnly && requested == null) {
      // Ordinary updates replace policies wholesale; omission must not erase the lock or, when
      // the entire policy object is omitted, unrelated policies.
      Policies policies =
          mapped.getPolicies() == null ? current.getPolicies() : mapped.getPolicies();
      return mapped.toBuilder().policies(policies.toBuilder().lockState(existing).build()).build();
    }
    return mapped;
  }

  private static LockState lockState(TableDto table) {
    return table == null || table.getPolicies() == null ? null : table.getPolicies().getLockState();
  }

  private static boolean isActiveSystemOnly(LockState lock) {
    return lock != null && lock.isLocked() && lock.getReason() == LockReason.SYSTEM_ONLY;
  }
}
