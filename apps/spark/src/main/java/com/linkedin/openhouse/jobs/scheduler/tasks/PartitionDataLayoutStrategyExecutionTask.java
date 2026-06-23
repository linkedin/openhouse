package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.TableDataLayoutMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Runs partition-scope data layout compaction for a table by launching {@code
 * PartitionDataCompactionSparkApp} with the table's selected partition strategies. The selected
 * partitions are passed as a {@code --partitionColumns} list plus one {@code --partitionValues}
 * tuple per partition.
 */
public class PartitionDataLayoutStrategyExecutionTask
    extends TableOperationTask<TableDataLayoutMetadata> {
  public static final JobConf.JobTypeEnum OPERATION_TYPE =
      JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_PARTITION_EXECUTION;

  public PartitionDataLayoutStrategyExecutionTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      TableDataLayoutMetadata metadata,
      long pollIntervalMs,
      long queuedTimeoutMs,
      long taskTimeoutMs) {
    super(jobsClient, tablesClient, metadata, pollIntervalMs, queuedTimeoutMs, taskTimeoutMs);
  }

  public PartitionDataLayoutStrategyExecutionTask(
      JobsClient jobsClient, TablesClient tablesClient, TableDataLayoutMetadata metadata) {
    super(jobsClient, tablesClient, metadata);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected List<String> getArgs() {
    List<DataLayoutStrategy> strategies = metadata.getDataLayoutStrategies();
    // All partition strategies of a table share the same partition columns; config defaults are the
    // same across them (the strategies table does not persist per-partition config).
    DataCompactionConfig config = strategies.get(0).getConfig();
    List<String> args =
        new ArrayList<>(
            Arrays.asList(
                "--tableName",
                metadata.fqtn(),
                "--partitionColumns",
                normalize(strategies.get(0).getPartitionColumns()),
                "--targetByteSize",
                Objects.toString(config.getTargetByteSize()),
                "--minByteSizeRatio",
                Objects.toString(config.getMinByteSizeRatio()),
                "--maxByteSizeRatio",
                Objects.toString(config.getMaxByteSizeRatio()),
                "--minInputFiles",
                Objects.toString(config.getMinInputFiles()),
                "--maxConcurrentFileGroupRewrites",
                Objects.toString(config.getMaxConcurrentFileGroupRewrites())));
    for (DataLayoutStrategy strategy : strategies) {
      args.add("--partitionValues");
      args.add(normalize(strategy.getPartitionId()));
    }
    if (config.isPartialProgressEnabled()) {
      args.add("--partialProgressEnabled");
      args.add("--partialProgressMaxCommits");
      args.add(Objects.toString(config.getPartialProgressMaxCommits()));
    }
    return args;
  }

  @Override
  protected boolean shouldRunTask() {
    return metadata.isPartitionScope() && metadata.isPrimary();
  }

  /**
   * Normalizes the generator's {@code ", "}-joined values into a clean {@code ","}-joined tuple.
   */
  private static String normalize(String commaSeparated) {
    if (commaSeparated == null) {
      return "";
    }
    return Arrays.stream(commaSeparated.split(",", -1))
        .map(String::trim)
        .collect(Collectors.joining(","));
  }
}
