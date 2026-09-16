package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.model.TableDto;

/** Protects cleanup lock metadata on writes that do not use the lock lifecycle API. */
final class LockPolicyValidator {
  private LockPolicyValidator() {}

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
