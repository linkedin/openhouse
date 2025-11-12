package com.linkedin.openhouse.common.stats.model;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Data model for openhouseTablePartitionsStats table.
 *
 * <p>Stores partition-level metadata and statistics such as null count, NaN count, row count, and
 * min/max values. Can represent both partition-level and table-level statistics.
 *
 * <p><b>Cardinality</b>: Each partition stats record references the latest commit that modified the
 * partition via commitMetadata.commitId (Foreign Key to {@link CommitEventTable}).
 *
 * <p>Stats are updated/replaced when new commits modify the partition.
 *
 * @see CommitEventTable
 * @see CommitEventTablePartitions
 * @see CommitMetadata
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartitionStats {

  /** Dataset information for this commit event */
  @NonNull private BaseEventModels.BaseTableIdentifier dataset;

  /**
   * Commit metadata for the latest commit that modified this partition.
   *
   * <p>The commitId within this metadata serves as a Foreign Key to {@link CommitEventTable}.
   */
  @NonNull private CommitMetadata commitMetadata;

  /**
   * Key-value mapping of partition columns and their corresponding values associated with the
   * statistics.
   *
   * <p>Can be null if the statistics is on a table level (non-partitioned table or table-level
   * aggregates).
   *
   * <p>Example for non-null partition: { "datepartition": "2025-01-25", "hourpartition": "12" }
   */
  @NonNull private Map<String, String> partitionData;

  /**
   * Total number of rows corresponding to the given partition specification if partition_spec is
   * not null; otherwise, the row count for the entire table.
   */
  private Long rowCount;

  /**
   * Total number of columns corresponding to the given partition specification if partition_spec is
   * not null; otherwise, the column count for the entire table.
   */
  private Long columnCount;

  /**
   * Stores null count statistics for each column in the dataset. Each element represents a column
   * name and its corresponding number of null values.
   */
  private List<ColumnStatistics> nullCount;

  /**
   * Stores NaN count statistics for each column in the dataset. Each element represents a column
   * name and its corresponding number of NaN values.
   *
   * <p>Applicable only to numeric data types such as float, double, or decimal.
   */
  private List<ColumnStatistics> nanCount;

  /**
   * Stores minimum value statistics for each column in the dataset. Each element represents a
   * column name and its corresponding minimum value.
   */
  private List<ColumnStatistics> minValue;

  /**
   * Stores maximum value statistics for each column in the dataset. Each element represents a
   * column name and its corresponding maximum value.
   */
  private List<ColumnStatistics> maxValue;

  /**
   * Stores column size in bytes statistics for each column in the dataset. Each element represents
   * a column name and its corresponding size in bytes.
   */
  private List<ColumnStatistics> columnSizeInBytes;

  /**
   * Timestamp (in epoch milliseconds) representing when the collector job processed and ingested
   * the corresponding event.
   */
  @NonNull private Long eventTimestampMs;

  /**
   * Column-level statistic interface for type-safe statistics.
   *
   * <p>Provides a type-safe way to represent column statistics with different value types. Each
   * implementation encapsulates a specific value type, ensuring compile-time type safety.
   *
   * <p>Implementations:
   *
   * <ul>
   *   <li>{@link LongColumnStatistics} - For counts (null, NaN) and sizes in bytes
   *   <li>{@link StringColumnStatistics} - For min/max of strings, dates, timestamps
   *   <li>{@link DoubleColumnStatistics} - For floating-point statistics
   * </ul>
   *
   * <p>This design pattern is inspired by Apache ORC's type-safe column statistics approach.
   */
  public interface ColumnStatistics {
    /**
     * Returns the column name this statistic applies to.
     *
     * @return the column name
     */
    String getColumnName();

    /**
     * Returns the statistic value as an object for generic handling.
     *
     * <p>The actual type depends on the implementation:
     *
     * <ul>
     *   <li>{@link LongColumnStatistics} returns {@link Long}
     *   <li>{@link StringColumnStatistics} returns {@link String}
     *   <li>{@link DoubleColumnStatistics} returns {@link Double}
     * </ul>
     *
     * @return the statistic value
     */
    Object getValue();
  }

  /** Long-valued column statistic for counts and sizes in bytes. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class LongColumnStatistics implements ColumnStatistics {
    /** Name of the column */
    @NonNull private String columnName;

    /** Long value for counts (null, NaN) and sizes in bytes */
    @NonNull private Long value;

    // Lombok generates: Long getValue() which satisfies Object getValue() from interface
  }

  /** String-valued column statistic for min/max of strings, dates, and timestamps. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class StringColumnStatistics implements ColumnStatistics {
    /** Name of the column */
    @NonNull private String columnName;

    /** String value for min/max of strings, dates, and timestamps */
    @NonNull private String value;

    // Lombok generates: String getValue() which satisfies Object getValue() from interface
  }

  /** Double-valued column statistic for floating-point min/max values. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class DoubleColumnStatistics implements ColumnStatistics {
    /** Name of the column */
    @NonNull private String columnName;

    /** Double value for floating-point statistics */
    @NonNull private Double value;

    // Lombok generates: Double getValue() which satisfies Object getValue() from interface
  }
}
