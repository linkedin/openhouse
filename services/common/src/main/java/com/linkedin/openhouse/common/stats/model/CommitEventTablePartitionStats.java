package com.linkedin.openhouse.common.stats.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * Data model for openhouseCommitEventTablePartitionStats table.
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
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class CommitEventTablePartitionStats extends BaseEventModels.BaseCommitEvent {

  /**
   * List of per-partition column values associated with the statistics.
   *
   * <p>Each element carries the partition column name and its typed value using {@link ColumnData}
   * implementations. The list order should align with the partition spec order for determinism.
   *
   * <p>Can be null if the statistics is on a table level (non-partitioned table or table-level
   * aggregates).
   *
   * <p>Example for non-null partition: [ new ColumnData.StringColumnData("datepartition",
   * "2025-01-25"), new ColumnData.StringColumnData("hourpartition", "12") ]
   */
  private List<ColumnData> partitionData;

  /**
   * Total number of rows corresponding to the given partition specification if partitionData is not
   * null; otherwise, the row count for the entire table.
   */
  private Long rowCount;

  /**
   * Total number of columns corresponding to the given partition specification if partitionData is
   * not null; otherwise, the column count for the entire table.
   */
  private Long columnCount;

  /**
   * Stores null count statistics for each column in the dataset. Each element represents a column
   * name and its corresponding number of null values.
   */
  private List<ColumnData> nullCount;

  /**
   * Stores NaN count statistics for each column in the dataset. Each element represents a column
   * name and its corresponding number of NaN values.
   *
   * <p>Applicable only to numeric data types such as float, double, or decimal.
   */
  private List<ColumnData> nanCount;

  /**
   * Stores minimum value statistics for each column in the dataset. Each element represents a
   * column name and its corresponding minimum value.
   */
  private List<ColumnData> minValue;

  /**
   * Stores maximum value statistics for each column in the dataset. Each element represents a
   * column name and its corresponding maximum value.
   */
  private List<ColumnData> maxValue;

  /**
   * Stores column size in bytes statistics for each column in the dataset. Each element represents
   * a column name and its corresponding size in bytes.
   */
  private List<ColumnData> columnSizeInBytes;
}
