package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.DatabaseMetadata;

/**
 * A task to perform delete multiple orphaned table directories and soft deleted tables within a
 * database. This differs from OrphanTableDirectoryDeletionTask in that it runs at a database level
 * and the job itself should be identifying and deleting the directories.
 */
public class TableDirectoryDeletionTask extends DatabaseOperationTask {
  public static final JobConf.JobTypeEnum OPERATION_TYPE =
      JobConf.JobTypeEnum.TABLE_DIRECTORY_DELETION;

  public TableDirectoryDeletionTask(
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
