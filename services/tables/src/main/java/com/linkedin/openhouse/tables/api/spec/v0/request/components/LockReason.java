package com.linkedin.openhouse.tables.api.spec.v0.request.components;

/** Structured lock reasons. Omitted or null reasons are interpreted as LEGACY. */
public enum LockReason {
  LEGACY,
  TIER3_AUTO_CLEANUP
}
