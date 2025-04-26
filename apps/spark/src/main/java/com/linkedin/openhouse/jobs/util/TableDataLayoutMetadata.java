package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class TableDataLayoutMetadata extends TableMetadata {
  protected List<DataLayoutStrategy> dataLayoutStrategies;
  protected boolean isPartitionScope;

  /** Helper function to copy a TableDataLayoutMetadata by replacing the dataLayoutStrategies. */
  public static TableDataLayoutMetadata from(
      TableDataLayoutMetadata metadata, List<DataLayoutStrategy> dataLayoutStrategies) {
    return TableDataLayoutMetadata.builder()
        .creator(metadata.getCreator())
        .dbName(metadata.getDbName())
        .tableName(metadata.getTableName())
        .creationTimeMs(metadata.getCreationTimeMs())
        .isPrimary(metadata.isPrimary())
        .isTimePartitioned(metadata.isTimePartitioned())
        .isClustered(metadata.isClustered())
        .jobExecutionProperties(metadata.getJobExecutionProperties())
        .retentionConfig(metadata.getRetentionConfig())
        .historyConfig(metadata.getHistoryConfig())
        .replicationConfig(metadata.getReplicationConfig())
        .dataLayoutStrategies(dataLayoutStrategies)
        .isPartitionScope(metadata.isPartitionScope)
        .build();
  }
}
