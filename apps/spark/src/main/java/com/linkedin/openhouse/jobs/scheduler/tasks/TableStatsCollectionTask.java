package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A task to collect iceberg stats on a table. */
public class TableStatsCollectionTask extends TableOperationTask<TableMetadata> {
  public static final JobConf.JobTypeEnum OPERATION_TYPE =
      JobConf.JobTypeEnum.TABLE_STATS_COLLECTION;

  public TableStatsCollectionTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      TableMetadata metadata,
      long pollIntervalMs,
      long queuedTimeoutMs,
      long taskTimeoutMs) {
    super(jobsClient, tablesClient, metadata, pollIntervalMs, queuedTimeoutMs, taskTimeoutMs);
  }

  public TableStatsCollectionTask(
      JobsClient jobsClient, TablesClient tablesClient, TableMetadata metadata) {
    super(jobsClient, tablesClient, metadata);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected List<String> getArgs() {
    return Stream.of("--tableName", metadata.fqtn()).collect(Collectors.toList());
  }

  @Override
  protected boolean shouldRun() {
    return true;
  }
}
