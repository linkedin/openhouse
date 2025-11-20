package com.linkedin.openhouse.common.stats.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * Data model for openhouseTableCommitEvents table.
 *
 * <p>Stores commit-level metadata for dataset changes. Each record represents a commit on table and
 * can be linked to multiple partition events via commitId.
 *
 * <p><b>Cardinality</b>: One commit event can have N partition events. See {@link
 * CommitEventTablePartitions} for partition-level details.
 *
 * @see CommitEventTablePartitions
 */
@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class CommitEventTable extends BaseEventModels.BaseCommitEvent {
  // All fields inherited from BaseCommitEvent and BaseTableIdentifier
  // datasetType is available via dataset.datasetType
}
