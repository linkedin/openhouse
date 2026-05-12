package com.linkedin.openhouse.analyzer.model;

/**
 * Analyzer-internal lifecycle states. The analyzer only writes {@link #PENDING}; the other values
 * are read off existing rows when deciding whether to re-issue a recommendation.
 *
 * <p>Intentionally separate from the wire-API and DB representations.
 */
public enum OperationStatus {
  PENDING,
  SCHEDULING,
  SCHEDULED,
  CANCELED
}
