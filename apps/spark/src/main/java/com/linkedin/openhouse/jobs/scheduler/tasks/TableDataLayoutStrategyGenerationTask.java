package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.Arrays;
import java.util.List;

/** A task to generate data layout strategies for a table. */
public class TableDataLayoutStrategyGenerationTask extends TableOperationTask<TableMetadata> {
  public static final JobConf.JobTypeEnum OPERATION_TYPE =
      JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_GENERATION;

  public TableDataLayoutStrategyGenerationTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      TableMetadata metadata,
      long pollIntervalMs,
      long timeoutMs) {
    super(jobsClient, tablesClient, metadata, pollIntervalMs, timeoutMs);
  }

  protected TableDataLayoutStrategyGenerationTask(
      JobsClient jobsClient, TablesClient tablesClient, TableMetadata metadata) {
    super(jobsClient, tablesClient, metadata);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected List<String> getArgs() {
    return Arrays.asList("--tableName", metadata.fqtn());
  }

  @Override
  protected boolean shouldRun() {
    return metadata.isPrimary();
  }
}
