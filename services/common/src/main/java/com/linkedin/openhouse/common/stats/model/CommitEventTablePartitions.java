package com.linkedin.openhouse.common.stats.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Data model for openhouseTableCommitEventPartitions table.
 *
 * <p>Stores partition-level information for each commit. Maps commit events to specific partitions
 * affected by that commit. One commit can correspond to multiple partition records. A commit event
 * represents a single commit-partition pair, capturing what changed for that specific partition in
 * that specific commit
 *
 * <p><b>Naming</b>: Represents "partitions of a commit event". This aligns with the conceptual
 * model where CommitEvent has a "partitions" field that's been normalized into a separate table.
 *
 * <p><b>Cardinality</b>: N partition records linked to 1 commit event via commitId foreign key.
 *
 * @see CommitEventTable
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CommitEventTablePartitions {

  /**
   * Foreign Key to {@link CommitEventTable}.
   *
   * <p>Establishes the parent-child relationship where one commit event can have multiple partition
   * records.
   */
  @NonNull private Long commitId;

  /**
   * Key-value mapping of partition columns and their corresponding values associated with the
   * commit.
   *
   * <p>Example: { "datepartition": "2025-01-25", "hourpartition": "12" }
   */
  @NonNull private Map<String, String> partitionData;

  /**
   * Timestamp (in epoch milliseconds) representing when the collector job processed and ingested
   * the corresponding event.
   */
  @NonNull private Long eventTimestampMs;
}
