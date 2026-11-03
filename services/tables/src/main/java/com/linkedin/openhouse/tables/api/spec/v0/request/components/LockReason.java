package com.linkedin.openhouse.tables.api.spec.v0.request.components;

/** Generic lock classifications used for reason-specific access enforcement. */
public enum LockReason {
  /** An active lock without a more specific classification. */
  LEGACY,
  /**
   * Restricts ordinary reads and writes while permitting system-declared operations subject to
   * existing authorization.
   */
  SYSTEM_ONLY
}
