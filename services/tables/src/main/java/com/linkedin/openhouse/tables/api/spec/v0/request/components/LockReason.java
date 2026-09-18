package com.linkedin.openhouse.tables.api.spec.v0.request.components;

/** Generic lock classifications. Recording a reason does not change access enforcement. */
public enum LockReason {
  /** An active lock without a more specific classification. */
  LEGACY,
  /**
   * Intended to restrict ordinary reads and writes while permitting system-declared operations
   * subject to existing authorization.
   */
  SYSTEM_ONLY
}
