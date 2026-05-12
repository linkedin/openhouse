package com.linkedin.openhouse.analyzer.model;

/**
 * Analyzer-internal enum for the operation types this app knows how to schedule. Intentionally
 * separate from the wire-API and DB representations so the analyzer can evolve its set of supported
 * operations without churning either boundary.
 */
public enum OperationType {
  ORPHAN_FILES_DELETION
}
