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
  }
}
