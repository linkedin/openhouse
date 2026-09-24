package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.SystemOnlyLockAccessDeniedException;
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

  /** Whether the table has an active lock that keeps the historical locked-table behavior. */
  static boolean isLegacyLocked(TableDto table) {
    LockState lock = lockState(table);
    return lock != null && lock.isLocked() && lock.getReason() != LockReason.SYSTEM_ONLY;
  }

  /** Preserve an active SYSTEM_ONLY lock; only the lock lifecycle API may change it. */
  static TableDto prepare(TableDto current, TableDto mapped) {
    LockState existing = lockState(current);
    if (!isActiveSystemOnly(existing)) {
      return mapped;
    }
    LockState requested = lockState(mapped);
    if (requested != null
        && (!requested.isLocked() || requested.getReason() != LockReason.SYSTEM_ONLY)) {
      throw new RequestValidationFailureException(
          "SYSTEM_ONLY lock state can only be changed through the lock lifecycle API.");
    }
    // Ordinary updates replace policies wholesale; omission must not erase the lock.
    Policies policies =
        mapped.getPolicies() == null ? Policies.builder().build() : mapped.getPolicies();
    return mapped.toBuilder().policies(policies.toBuilder().lockState(existing).build()).build();
  }

  private static LockState lockState(TableDto table) {
    return table == null || table.getPolicies() == null ? null : table.getPolicies().getLockState();
  }

  private static boolean isActiveSystemOnly(LockState lock) {
    return lock != null && lock.isLocked() && lock.getReason() == LockReason.SYSTEM_ONLY;
  }
}
