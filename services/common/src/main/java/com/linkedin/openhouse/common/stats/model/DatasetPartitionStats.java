package com.linkedin.openhouse.common.stats.model;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data model for openhouseDatasetPartitionsStats table.
 *
 * <p>Stores partition-level metadata and statistics such as null count, NaN count, row count, and
 * min/max values for the table. Can represent both partition-level and table-level statistics.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasetPartitionStats {

  // ==================== Basic Table Identification ====================

  /** Name of the database for the dataset */
  private String databaseName;

  /** Name of the table for the dataset */
  private String tableName;

  /** Name of the cluster (e.g., holdem/war) */
  private String clusterName;

  // ==================== Partition Information ====================

  /**
   * Key-value mapping of partition columns and their corresponding values associated with the
   * statistics.
   *
   * <p>Can be null if the statistics is on a table level (non-partitioned table or table-level
   * aggregates).
   *
   * <p>Example for non-null partition: { "datepartition": "2025-01-25", "hourpartition": "12" }
   */
  private Map<String, String> partitionSpec;

  // ==================== Commit Information ====================

  /** Latest commit that changed this partition */
  private String commitId;

  /** Timestamp of the latest commit event captured in epoch milliseconds */
  private Long commitTimestampInEpochMs;

  /**
   * Unique application identifier (e.g., Spark Application ID) associated with the process or job
   * that performed the commit
   */
  private String commitAppId;

  /**
   * Descriptive name of the application or job that executed the commit. Helps in identifying the
   * pipeline or workflow responsible for the data change.
   */
  private String commitAppName;

  /** Type of operation performed during the commit (e.g., APPEND, OVERWRITE, DELETE) */
  private String commitOperation;

  // ==================== Partition-Level Statistics ====================

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
   *
   * <p>Example: [ { "columnName": "user_id", "longValue": 0 }, { "columnName": "email",
   * "longValue": 12 }, { "columnName": "age", "longValue": 3 } ]
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

  // ==================== Event Processing Metadata ====================

  /**
   * Timestamp (in epoch milliseconds) representing when the collector job processed and ingested
   * the corresponding commit event.
   */
  private Long eventTimestampInEpochMs;

  // ==================== Inner Class for Statistics ====================

  /**
   * Inner class representing column statistics. Used for all column-level statistics (null count,
   * NaN count, min/max values, size). Supports multiple value types to maintain type safety while
   * allowing flexibility for different statistical measures.
   *
   * <p>Type-specific fields allow compile-time type checking and prevent runtime serialization
   * errors. Clients should use the appropriate field based on the statistic type:
   *
   * <ul>
   *   <li>longValue: for counts (null count, NaN count) and sizes in bytes
   *   <li>stringValue: for min/max values of string columns or date representations
   *   <li>doubleValue: for floating-point statistics if needed in future
   * </ul>
   *
   * <p>Only one value field should be populated per instance based on the statistic being
   * represented.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ColumnStatistic {
    /** Name of the column */
    private String columnName;

    /**
     * Long value for numeric statistics (counts, sizes in bytes, etc.). Use this for:
     *
     * <ul>
     *   <li>Null count
     *   <li>NaN count
     *   <li>Column size in bytes
     * </ul>
     */
    private Long longValue;

    /**
     * String value for textual statistics (min/max of string columns, dates, etc.). Use this for:
     *
     * <ul>
     *   <li>Min/max values of string columns
     *   <li>Date representations
     *   <li>Any non-numeric statistics
     * </ul>
     */
    private String stringValue;

    /**
     * Double value for floating-point statistics. Reserved for future use. Use this for:
     *
     * <ul>
     *   <li>Floating-point aggregates
     *   <li>Statistical measures requiring decimal precision
     * </ul>
     */
    private Double doubleValue;
  }
}
