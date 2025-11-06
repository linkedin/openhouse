package com.linkedin.openhouse.common.stats.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Data model for openhouseDatasetPartitionsCommitEvents table.
 *
 * <p>Stores partition-level information for each commit. Maps commit events to specific partitions
 * affected by that commit. One commit can correspond to multiple partition records.
 *
 * <p><b>Cardinality</b>: N partition events linked to 1 commit event via commitId foreign key.
 * Table metadata is obtained by joining with {@link DatasetCommitEvent}.
 *
 * <p>This is a standalone model (no inheritance) as it only needs event timestamp and commit
 * reference, without table metadata.
 *
 * @see DatasetCommitEvent
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasetPartitionCommitEvent {

  /**
   * Unique identifier for each commit event.
   *
   * <p><b>Foreign Key</b>: References {@link DatasetCommitEvent#commitId}. This establishes a
   * parent-child relationship where one commit event can have multiple partition commit events.
   *
   * <p>This FK relationship enables joining partition-level commit details with the parent commit
   * metadata (table identification, commit timestamp, operation type, etc.).
   */
  @NonNull private String commitId;

  /**
   * Key-value mapping of partition columns and their corresponding values associated with the
   * commit.
   *
   * <p>Example: { "datepartition": "2025-01-25", "hourpartition": "12" }
   */
  @NonNull private Map<String, String> partitionSpec;

  /**
   * Timestamp (in epoch milliseconds) representing when the collector job processed and ingested
   * the corresponding event.
   */
  @NonNull private Long eventTimestampInEpochMs;
}
