package com.linkedin.openhouse.optimizer.analyzer;

import java.util.Locale;

/**
 * Selects which scan a single analyzer process performs. Chosen via {@code analyzer.mode} so the
 * two paths can be deployed as two independent instances (e.g. two K8s CronJobs) on different
 * cadences.
 */
public enum AnalyzerMode {
  /** Evaluate only tables changed since the last run. Frequent and cheap (e.g. every 30 min). */
  INCREMENTAL,

  /**
   * Scan every database/table. Infrequent (e.g. daily). Catches tables an incremental scan misses —
   * idle tables and tables whose <i>commit failed</i> (which leaves orphan files but publishes no
   * stats, so {@code updated_at} does not advance) — so their TTL-based cleanup still fires.
   */
  FULL;

  /** Parse a config value case-insensitively; defaults to {@link #INCREMENTAL} when blank. */
  public static AnalyzerMode from(String value) {
    if (value == null || value.trim().isEmpty()) {
      return INCREMENTAL;
    }
    try {
      return AnalyzerMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid analyzer.mode '" + value + "'; expected INCREMENTAL or FULL", e);
    }
  }
}
