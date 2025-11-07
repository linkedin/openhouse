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
  public static class BaseDataset {

    /** Name of the database for the dataset */
    @NonNull private String databaseName;

    /** Name of the table for the dataset */
    @NonNull private String tableName;

    /** Name of the cluster (e.g., holdem/war) */
    @NonNull private String clusterName;
  }

  /** Base class for commit events that contains dataset information. */
  @Data
  @SuperBuilder
  @NoArgsConstructor
  @AllArgsConstructor
  public abstract static class BaseCommitEvent {

    /** Dataset information for this commit event */
    @NonNull private BaseDataset dataset;

    /** Unique identifier for each commit event */
    @NonNull private String commitId;

    /** Timestamp of the commit event captured in epoch milliseconds */
    @NonNull private Long commitTimestampMs;

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

    /** Type of operation performed during the commit (e.g., APPEND, OVERWRITE, DELETE, REPLACE) */
    @NonNull private CommitOperation commitOperation;
  }
}
