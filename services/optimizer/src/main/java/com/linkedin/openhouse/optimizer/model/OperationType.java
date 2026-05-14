package com.linkedin.openhouse.optimizer.model;

/**
 * Internal enum for the operation types the analyzer and scheduler know about. Intentionally
 * separate from the wire-API and DB representations so the internal model can evolve its set of
 * supported operations without churning either boundary.
 */
public enum OperationType {
  ORPHAN_FILES_DELETION
}
