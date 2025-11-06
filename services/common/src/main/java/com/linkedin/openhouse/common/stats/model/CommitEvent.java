package com.linkedin.openhouse.common.stats.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

/**
 * Data model for openhouseDatasetCommitEvents table.
 *
 * <p>Stores commit-level metadata for dataset changes. Each record represents a single commit event
 * and can be linked to multiple partition events via commitId.
 *
 * <p><b>Cardinality</b>: One commit event can have N partition events. See {@link
 * CommitEventPartitions} for partition-level details.
 *
 * @see CommitEventPartitions
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class CommitEvent extends BaseEventModels.BaseCommitEvent {

  /**
   * Type of dataset: PARTITIONED or NON_PARTITIONED.
   *
   * <p>Used to distinguish between partitioned tables (with partition-level stats) and
   * non-partitioned tables (with table-level stats only).
   */
  @NonNull private DatasetType datasetType;

  /**
   * Timestamp (in epoch milliseconds) representing when the collector job processed and ingested
   * the corresponding event.
   */
  @NonNull private Long eventTimestampInEpochMs;
}
