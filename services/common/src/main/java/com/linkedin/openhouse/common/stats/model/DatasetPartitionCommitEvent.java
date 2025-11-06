package com.linkedin.openhouse.common.stats.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data model for openhouseDatasetPartitionsCommitEvents table.
 *
 * <p>Stores partition-level information for each commit. Maps commit events to specific partitions
 * affected by that commit. One commit can correspond to multiple partition records.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatasetPartitionCommitEvent {

  // ==================== Commit Reference ====================

  /**
   * Unique identifier for each commit event (Foreign Key to DatasetCommitEvent.commitId). Links
   * this partition event to its parent commit.
   */
  private String commitId;

  // ==================== Partition Information ====================

  /**
   * Key-value mapping of partition columns and their corresponding values associated with the
   * commit.
   *
   * <p>Example: { "datepartition": "2025-01-25", "hourpartition": "12" }
   */
  private Map<String, String> partitionSpec;

  // ==================== Event Processing Metadata ====================

  /**
   * Timestamp (in epoch milliseconds) representing when the collector job processed and ingested
   * the corresponding commit event.
   */
  private Long eventTimestampEpochMs;
}
