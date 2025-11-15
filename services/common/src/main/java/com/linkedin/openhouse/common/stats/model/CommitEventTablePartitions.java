package com.linkedin.openhouse.common.stats.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

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
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class CommitEventTablePartitions extends BaseEventModels.BaseCommitEvent {

  /**
   * List of per-partition column values associated with the commit.
   *
   * <p>Each element carries the partition column name and its typed value using {@link ColumnData}
   * implementations. The list order should align with the partition spec order for determinism.
   *
   * <p>Example: [ new ColumnData.StringColumnData("datepartition", "2025-01-25"), new
   * ColumnData.StringColumnData("hourpartition", "12") ]
   */
  @NonNull private List<ColumnData> partitionData;
}
