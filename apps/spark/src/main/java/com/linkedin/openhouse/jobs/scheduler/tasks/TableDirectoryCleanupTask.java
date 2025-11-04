package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.DatabaseMetadata;

public class TableDirectoryCleanupTask extends DatabaseOperationTask {
  public static final JobConf.JobTypeEnum OPERATION_TYPE =
      JobConf.JobTypeEnum.TABLE_DIRECTORY_CLEANUP;

  public TableDirectoryCleanupTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      DatabaseMetadata metadata,
      long pollIntervalMs,
      long queuedTimeoutMs,
      long taskTimeoutMs) {
    super(jobsClient, tablesClient, metadata, pollIntervalMs, queuedTimeoutMs, taskTimeoutMs);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected boolean shouldRunTask() {
    return true;
  }
}
