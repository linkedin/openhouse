package com.linkedin.openhouse.optimizer.model;

/**
 * Internal lifecycle states for an operation. The analyzer writes {@link #PENDING}; the scheduler
 * transitions through {@link #SCHEDULING} and {@link #SCHEDULED}. {@link #CANCELED} marks
 * deduplicated PENDING rows.
 *
 * <p>Intentionally separate from the wire-API and DB representations.
 */
public enum OperationStatus {
  PENDING,
  SCHEDULING,
  SCHEDULED,
  CANCELED
}
