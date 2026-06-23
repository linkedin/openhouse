package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Metadata for a table's data layout optimization. Holds the list of {@link DataLayoutStrategy} to
 * be executed for the table. For table-scope compaction the list contains a single table-level
 * strategy; for partition-scope compaction ({@code isPartitionScope == true}) the list contains the
 * per-partition strategies selected for the table.
 */
@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class TableDataLayoutMetadata extends TableMetadata {
  @NonNull protected List<DataLayoutStrategy> dataLayoutStrategies;
  protected boolean isPartitionScope;
}
