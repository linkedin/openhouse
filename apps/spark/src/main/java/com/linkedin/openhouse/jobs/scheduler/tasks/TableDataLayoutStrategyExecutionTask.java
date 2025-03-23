package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.datalayout.config.DataCompactionConfig;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.TableDataLayoutMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class TableDataLayoutStrategyExecutionTask
    extends TableOperationTask<TableDataLayoutMetadata> {
  public static final JobConf.JobTypeEnum OPERATION_TYPE =
      JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_EXECUTION;

  public TableDataLayoutStrategyExecutionTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      TableDataLayoutMetadata metadata,
      long pollIntervalMs,
      long timeoutMs) {
    super(jobsClient, tablesClient, metadata, pollIntervalMs, timeoutMs);
  }

  public TableDataLayoutStrategyExecutionTask(
      JobsClient jobsClient, TablesClient tablesClient, TableDataLayoutMetadata metadata) {
    super(jobsClient, tablesClient, metadata);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected List<String> getArgs() {
    DataCompactionConfig config = metadata.getDataLayoutStrategy().getConfig();
    List<String> args =
        new ArrayList<>(
            Arrays.asList(
                "--tableName",
                metadata.fqtn(),
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
    if (config.isPartialProgressEnabled()) {
      args.add("--partialProgressEnabled");
      args.add("--partialProgressMaxCommits");
      args.add(Objects.toString(config.getPartialProgressMaxCommits()));
    }
    return args;
  }

  @Override
  protected boolean shouldRun() {
    return metadata.isPrimary();
  }
}
