package com.linkedin.openhouse.analyzer.model;

/**
 * Analyzer-internal lifecycle outcomes for a completed operation. Mirrors the values written to
 * {@code table_operations_history.status}; parsed at the boundary so the analyzer can switch on a
 * typed value instead of comparing strings.
 *
 * <p>Intentionally separate from the wire-API and DB representations.
 */
public enum HistoryStatus {
  SUCCESS,
  FAILED
}
