package com.linkedin.openhouse.common.stats.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Column-level data interface for type-safe values used in events and stats.
 *
 * <p>Implementations encapsulate a specific value type to provide compile-time safety.
 */
public interface ColumnData {
  /** Returns the column name this data applies to. */
  String getColumnName();

  /** Returns the underlying value. */
  Object getValue();

  /** Long-valued column data for counts and sizes in bytes. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class LongColumnData implements ColumnData {
    @NonNull private String columnName;
    @NonNull private Long value;
  }

  /** String-valued column data for min/max of strings, dates, and timestamps. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class StringColumnData implements ColumnData {
    @NonNull private String columnName;
    @NonNull private String value;
  }

  /** Double-valued column data for floating-point min/max values. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class DoubleColumnData implements ColumnData {
    @NonNull private String columnName;
    @NonNull private Double value;
  }
}
