package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.exception.CleanupLockAccessDeniedException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import com.linkedin.openhouse.common.utils.SystemActionContext;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.model.TableDto;

/** Enforces data-access lock rules and protects lock metadata outside the lifecycle API. */
final class LockPolicyValidator {
  private LockPolicyValidator() {}

  /** Evaluate only after the caller's data-access authorization succeeds. */
  static void checkCleanupAccess(TableDto table) {
    LockState lock = lockState(table);
    if (isCleanup(lock) && lock.isLocked() && !SystemActionContext.isEnabled()) {
      String message = lock.getMessage();
      String detail = message == null || message.trim().isEmpty() ? "" : ": " + message;
      throw new CleanupLockAccessDeniedException(
          String.format(
              "Table %s.%s is locked for TIER3_AUTO_CLEANUP%s. Promote the table to Tier 2 to retain it, "
                  + "or use the reason-targeted OpenHouse unlock endpoint as an authorized lock administrator.",
              table.getDatabaseId(), table.getTableId(), detail));
    }
  }

  static void checkWrite(TableDto table) {
    LockState lock = lockState(table);
    if (lock == null || !lock.isLocked()) {
      return;
    }
    if (isCleanup(lock)) {
      checkCleanupAccess(table);
    } else {
      throw new UnsupportedClientOperationException(
          UnsupportedClientOperationException.Operation.LOCKED_TABLE_OPERATION,
          String.format(
              "Table %s.%s is in locked state and cannot be written to",
              table.getDatabaseId(), table.getTableId()));
    }
  }

  static TableDto prepare(TableDto current, TableDto mapped) {
    LockState existing = lockState(current);
    LockState requested = lockState(mapped);
    boolean existingCleanup = isCleanup(existing);
    if ((existingCleanup || isCleanup(requested))
        && requested != null
        && !requested.equals(existing)) {
      throw new RequestValidationFailureException(
          "Cleanup lock state can only be changed through the lock lifecycle API.");
    }
    if (existingCleanup && requested == null) {
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

  private static boolean isCleanup(LockState lock) {
    return lock != null && lock.getReason() == LockReason.TIER3_AUTO_CLEANUP;
  }
}
