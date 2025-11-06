package com.linkedin.openhouse.common.stats.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data model for openhouseDatasetCommitEvents table.
 *
 * <p>Stores metadata for each commit event in a table. Represents commit-level information without
 * partition or statistics details.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasetCommitEvent {

  // ==================== Basic Table Identification ====================

  /** Name of the database for the dataset */
  private String databaseName;

  /** Name of the table for the dataset */
  private String tableName;

  /** Name of the cluster (e.g., holdem/war) */
  private String clusterName;

  // ==================== Commit Event Metadata ====================

  /** Type of dataset (PARTITIONED or NON_PARTITIONED) */
  private String datasetType;

  /** Unique identifier for each commit event (Primary Key) */
  private String commitId;

  /**
   * Timestamp of the commit event, captured in epoch milliseconds. Represents when the actual
   * commit occurred.
   */
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

  /** Type of operation performed during the commit (e.g., APPEND, OVERWRITE, DELETE, REPLACE). */
  private String commitOperation;

  // ==================== Event Processing Metadata ====================

  /**
   * Timestamp (in epoch milliseconds) representing when the collector job processed and ingested
   * the corresponding commit event.
   */
  private Long eventTimestampInEpochMs;
}
