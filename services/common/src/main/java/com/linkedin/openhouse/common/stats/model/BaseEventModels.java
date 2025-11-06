package com.linkedin.openhouse.common.stats.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

/**
 * Container for base model classes used in the dataset event hierarchy.
 *
 * <p>This file contains all abstract base classes that provide common fields and functionality for
 * dataset events and table metadata. The hierarchy is simplified with minimal inheritance:
 *
 * <pre>
 * BaseTableIdentifier (abstract)
 * ├── databaseName, tableName, clusterName
 * │
 * └─→ BaseDatasetCommitEvent (abstract)
 *     ├── commitId, commitTimestampInEpochMs
 *     ├── commitAppId, commitAppName, commitOperation
 *     │
 *     ├─→ DatasetCommitEvent (concrete)
 *     │   └── + eventTimestampInEpochMs, datasetType
 *     │
 *     └─→ DatasetPartitionStats (concrete)
 *         └── + eventTimestampInEpochMs, statistics fields
 *
 * DatasetPartitionCommitEvent (concrete - standalone)
 * └── eventTimestampInEpochMs, commitId, partitionSpec
 * </pre>
 *
 * <p><b>Note</b>: {@code BaseTableIdentifier} differs from the standalone {@link BaseTableMetadata}
 * class, which is used by {@link IcebergTableStats} and contains additional metadata fields.
 */
public final class BaseEventModels {

  private BaseEventModels() {
    // Utility class - no instantiation
  }

  // ==================== Base Table Identifier ====================

  /**
   * Base class containing table identification metadata for dataset events.
   *
   * <p>Contains common fields for identifying a specific table (database, table, cluster). This can
   * be extended by any model that needs to track table-level information.
   *
   * <p><b>Note</b>: This class differs from the standalone {@link BaseTableMetadata} which contains
   * additional metadata fields (UUID, location, timestamps, etc.) and is used by {@link
   * IcebergTableStats}.
   */
  @Data
  @SuperBuilder
  @NoArgsConstructor
  @AllArgsConstructor
  public abstract static class BaseTableIdentifier {

    /** Name of the database for the dataset */
    @NonNull private String databaseName;

    /** Name of the table for the dataset */
    @NonNull private String tableName;

    /** Name of the cluster (e.g., holdem/war) */
    @NonNull private String clusterName;
  }

  // ==================== Base Dataset Commit Event ====================

  /**
   * Base class for dataset commit events that include table identification and commit metadata.
   *
   * <p>Extends {@link BaseTableIdentifier} to include table identification and adds commit-related
   * fields. Concrete models should add their own {@code eventTimestampInEpochMs} field.
   *
   * <p>This is used by models that need to track commit information along with table context.
   */
  @Data
  @SuperBuilder
  @NoArgsConstructor
  @AllArgsConstructor
  @EqualsAndHashCode(callSuper = true)
  public abstract static class BaseDatasetCommitEvent extends BaseTableIdentifier {

    /**
     * Unique identifier for each commit event.
     *
     * <p>This field serves as the primary key for {@link DatasetCommitEvent} and as a foreign key
     * in {@link DatasetPartitionCommitEvent} and {@link DatasetPartitionStats}.
     */
    @NonNull private String commitId;

    /** Timestamp of the commit event captured in epoch milliseconds */
    @NonNull private Long commitTimestampInEpochMs;

    /**
     * Unique application identifier (e.g., Spark Application ID) associated with the process or job
     * that performed the commit
     */
    @NonNull private String commitAppId;

    /**
     * Descriptive name of the application or job that executed the commit. Helps in identifying the
     * pipeline or workflow responsible for the data change.
     */
    @NonNull private String commitAppName;

    /** Type of operation performed during the commit (e.g., APPEND, OVERWRITE, DELETE, REPLACE) */
    @NonNull private CommitOperation commitOperation;
  }
}
