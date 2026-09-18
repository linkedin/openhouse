package com.linkedin.openhouse.tables.api.spec.v0.request.components;

/**
 * Structured lock reasons. LEGACY identifies the unqualified lock that the bare lock and unlock
 * routes manage, and an omitted or null reason resolves to it.
 */
public enum LockReason {
  LEGACY,
  TIER3_AUTO_CLEANUP
}
