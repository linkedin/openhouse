package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.DatabaseMetadata;

public class SoftDeletedTableCleanupTask extends DatabaseOperationTask {
  public static final JobConf.JobTypeEnum OPERATION_TYPE =
      JobConf.JobTypeEnum.SOFT_DELETED_TABLE_CLEANUP;

  public SoftDeletedTableCleanupTask(
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
  // TODO: limit this to tables that have soft deleted tables enabled
  protected boolean shouldRunTask() {
    return true;
  }
}
