package com.linkedin.openhouse.common.stats.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/** Standalone class representing commit metadata. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CommitMetadata {

  /** Unique identifier for the commit event */
  @NonNull private String commitId;

  /** Timestamp of the commit event captured in epoch milliseconds */
  @NonNull private Long commitTimestampInEpochMs;

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
