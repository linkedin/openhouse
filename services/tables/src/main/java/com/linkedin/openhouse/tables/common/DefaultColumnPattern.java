package com.linkedin.openhouse.tables.common;

/**
 * ENUM for Date pattern associated with ColumnName in {@link
 * com.linkedin.openhouse.tables.api.spec.v0.request.components.RetentionColumnPattern}
 */
public enum DefaultColumnPattern {
  // default date pattern for datasets
  DAY("yyyy-MM-dd"),
  HOUR("yyyy-MM-dd-HH");

  private final String pattern;

  DefaultColumnPattern(String pattern) {
    this.pattern = pattern;
  }

  public String getPattern() {
    return pattern;
  }
}
