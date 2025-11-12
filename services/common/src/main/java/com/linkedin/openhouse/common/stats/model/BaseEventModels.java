package com.linkedin.openhouse.common.stats.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

/** Container for base model classes used in the dataset event hierarchy. */
public final class BaseEventModels {

  private BaseEventModels() {
    // Utility class - no instantiation
  }

  /** Base class containing table identification metadata. */
  @Data
  @SuperBuilder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class BaseTableIdentifier {

    /** Name of the database for the dataset */
    @NonNull private String databaseName;

    /** Name of the table for the dataset */
    @NonNull private String tableName;

    /** Name of the cluster (e.g., holdem/war) */
    @NonNull private String clusterName;

    /**
     * Fully qualified path to the root table metadata JSON. Useful to distinguish uniqueness in
     * case the table is dropped and recreated with the same name.
     */
    @NonNull private String tableMetadataLocation;

    /**
     * String representation of the Iceberg PartitionSpec for the dataset.
     *
     * <p>Use the string from PartitionSpec.toString() to describe partitioning. For non-partitioned
     * tables, use PartitionSpec.UNPARTITIONED_SPEC.toString(). See:
     * https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/PartitionSpec.java
     */
    @NonNull private String partitionSpec;
  }

  /** Base class for commit events that contains dataset information. */
  @Data
  @SuperBuilder
  @NoArgsConstructor
  @AllArgsConstructor
  public abstract static class BaseCommitEvent {

    /** Dataset information for this commit event */
    @NonNull private BaseTableIdentifier dataset;

    /** commit Metadata for this commit */
    @NonNull private CommitMetadata commitMetadata;

    /**
     * Timestamp (in epoch milliseconds) representing when the collector job processed and ingested
     * the corresponding event.
     */
    @NonNull private Long eventTimestampMs;
  }
}
