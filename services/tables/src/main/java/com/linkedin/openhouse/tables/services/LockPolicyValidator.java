package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.model.TableDto;

/**
 * Classifies lock state and protects SYSTEM_ONLY lock metadata outside the lifecycle API.
 * SYSTEM_ONLY data-access checks are in {@link
 * com.linkedin.openhouse.tables.utils.AuthorizationUtils#checkSystemOnlyLockAccess}.
 */
final class LockPolicyValidator {
  private LockPolicyValidator() {}

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
