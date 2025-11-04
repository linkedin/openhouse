package com.linkedin.openhouse.common.stats.model;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Unified data model for capturing Iceberg commit events and partition-level statistics.
 *
 * <p>This model combines: 1. Commit event metadata (snapshot-level information) 2. Partition-level
 * commit events 3. Partition-level statistics (null count, NaN count, min/max values, etc.)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IcebergCommitEventStats {

  // ==================== Basic Table Identification ====================

  /** Cluster name */
  private String clusterName;

  /** Database name of the dataset */
  private String databaseName;

  /** Table name of the dataset */
  private String tableName;

  // ==================== Commit Event Metadata ====================

  /** Type of dataset (PARTITIONED or NON_PARTITIONED) */
  private String datasetType;

  /** Unique identifier for the commit/snapshot (primary key) */
  private String commitId;

  /** Timestamp of the commit event in epoch milliseconds */
  private Long commitTimestampEpochMs;

  /** Application ID that performed the commit (e.g., Spark Application ID) */
  private String commitAppId;

  /** Application name that performed the commit */
  private String commitAppName;

  /** Operation type (e.g., APPEND, OVERWRITE, DELETE, REPLACE) */
  private String commitOperation;

  /** Timestamp when the collector job processed this event */
  private Long eventTimestampInEpochMs;

  // ==================== Partition-Level Commit Information ====================

  /**
   * Key-value mapping of partition columns and their corresponding values. Can be null if the
   * statistics is on a table level (non-partitioned table). Example: {"datepartition":
   * "2025-01-25", "hourpartition": "12"}
   */
  private Map<String, String> partitionSpec;

  // ==================== Partition-Level Statistics ====================

  /**
   * Total number of rows for the given partition specification if partitionSpec is not null;
   * otherwise, the row count for the entire table.
   */
  private Long rowCount;

  /**
   * Total number of columns for the given partition specification if partitionSpec is not null;
   * otherwise, the column count for the entire table.
   */
  private Long columnCount;

  /**
   * Stores null count statistics for each column in the dataset. Each element represents a column
   * name and its corresponding number of null values.
   */
  private List<ColumnStatistic> nullCount;

  /**
   * Stores NaN count statistics for each column in the dataset. Each element represents a column
   * name and its corresponding number of NaN values. Applicable only to numeric data types such as
   * float, double, or decimal.
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

  // ==================== Inner Class for Statistics ====================

  /**
   * Inner class representing column statistics. Used for all column-level statistics (null count,
   * NaN count, min/max values, size). Value is stored as String for flexibility - can be
   * transformed before emitting event.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ColumnStatistic {
    /** Name of the column */
    private String columnName;

    /** Statistical value as string (count, min/max value, size in bytes, etc.) */
    private String value;
  }
}
