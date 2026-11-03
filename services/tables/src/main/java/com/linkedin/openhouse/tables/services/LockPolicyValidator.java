package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Optional;

/**
 * Classifies lock state and protects SYSTEM_ONLY lock metadata outside the lifecycle API.
 * SYSTEM_ONLY data-access checks are in {@link
 * com.linkedin.openhouse.tables.utils.AuthorizationUtils#checkSystemOnlyLockAccess}.
 */
final class LockPolicyValidator {
  private LockPolicyValidator() {}

  /** Whether the table has an active lock that keeps the historical locked-table behavior. */
  static boolean isLegacyLocked(TableDto table) {
    return lockState(table)
        .filter(lock -> lock.isLocked() && lock.getReason() != LockReason.SYSTEM_ONLY)
        .isPresent();
  }

  /** Preserve an active SYSTEM_ONLY lock; only the lock lifecycle API may change it. */
  static TableDto prepare(TableDto current, TableDto mapped) {
    Optional<LockState> existing =
        lockState(current).filter(LockPolicyValidator::isActiveSystemOnly);
    if (!existing.isPresent()) {
      return mapped;
    }
    if (lockState(mapped).filter(lock -> !isActiveSystemOnly(lock)).isPresent()) {
      throw new RequestValidationFailureException(
          "SYSTEM_ONLY lock state can only be changed through the lock lifecycle API.");
    }
    // Ordinary updates replace policies wholesale; omission must not erase the lock.
    Policies policies =
        Optional.ofNullable(mapped.getPolicies()).orElseGet(() -> Policies.builder().build());
    return mapped
        .toBuilder()
        .policies(policies.toBuilder().lockState(existing.get()).build())
        .build();
  }

  private static Optional<LockState> lockState(TableDto table) {
    return Optional.ofNullable(table).map(TableDto::getPolicies).map(Policies::getLockState);
  }

  private static boolean isActiveSystemOnly(LockState lock) {
    return lock.isLocked() && lock.getReason() == LockReason.SYSTEM_ONLY;
  }
}
