package com.linkedin.openhouse.common.stats.model;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

/**
 * Data model for openhouseDatasetPartitionsStats table.
 *
 * <p>Stores partition-level metadata and statistics such as null count, NaN count, row count, and
 * min/max values. Can represent both partition-level and table-level statistics.
 *
 * <p><b>Cardinality</b>: Each partition stats record references the latest commit that modified the
 * partition via commitMetadata.commitId (Foreign Key to {@link CommitEvent}).
 *
 * <p>Stats are updated/replaced when new commits modify the partition.
 *
 * @see CommitEvent
 * @see CommitEventPartitions
 * @see CommitMetadata
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class PartitionStats extends BaseEventModels.BaseDataset {

  /**
   * Commit metadata for the latest commit that modified this partition.
   *
   * <p>The commitId within this metadata serves as a Foreign Key to {@link CommitEvent}.
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
  private List<ColumnStatistic> nullCount;

  /**
   * Stores NaN count statistics for each column in the dataset. Each element represents a column
   * name and its corresponding number of NaN values.
   *
   * <p>Applicable only to numeric data types such as float, double, or decimal.
   */
  private List<ColumnStatistic> nanCount;

  /**
   * Stores minimum value statistics for each column in the dataset. Each element represents a
   * column name and its corresponding minimum value.
   */
  private List<ColumnStatistic> minValue;

  /**
   * Stores maximum value statistics for each column in the dataset. Each element represents a
   * column name and its corresponding maximum value.
   */
  private List<ColumnStatistic> maxValue;

  /**
   * Stores column size in bytes statistics for each column in the dataset. Each element represents
   * a column name and its corresponding size in bytes.
   */
  private List<ColumnStatistic> columnSizeInBytes;

  /**
   * Timestamp (in epoch milliseconds) representing when the collector job processed and ingested
   * the corresponding event.
   */
  @NonNull private Long eventTimestampInEpochMs;

  /**
   * Column-level statistic with type-specific value fields.
   *
   * <p>Type-specific fields provide compile-time type safety:
   *
   * <ul>
   *   <li><b>longValue</b> - For counts (null, NaN) and sizes in bytes
   *   <li><b>stringValue</b> - For min/max of strings, dates, timestamps
   *   <li><b>doubleValue</b> - For floating-point statistics (future use)
   * </ul>
   *
   * <p>Only populate one value field per instance.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ColumnStatistic {
    /** Name of the column */
    @NonNull private String columnName;

    /** Long value for counts (null, NaN) and sizes in bytes */
    private Long longValue;

    /** String value for min/max of strings, dates, and timestamps */
    private String stringValue;

    /** Double value for floating-point statistics (reserved for future use) */
    private Double doubleValue;
  }
}
