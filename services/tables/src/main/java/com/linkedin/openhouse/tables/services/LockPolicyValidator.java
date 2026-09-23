package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockReason;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.model.TableDto;

/** Protects system-only lock metadata on writes that do not use the lock lifecycle API. */
final class LockPolicyValidator {
  private LockPolicyValidator() {}

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
