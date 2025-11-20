package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.util.DatabaseMetadata;
import java.util.Arrays;
import java.util.List;

/**
 * A callable class to apply an operation to a full database by running a job. Takes care of the job
 * lifecycle using /jobs API.
 */
public abstract class DatabaseOperationTask extends OperationTask<DatabaseMetadata> {

  public DatabaseOperationTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      DatabaseMetadata metadata,
      long pollIntervalMs,
      long queuedTimeoutMs,
      long taskTimeoutMs) {
    super(jobsClient, tablesClient, metadata, pollIntervalMs, queuedTimeoutMs, taskTimeoutMs);
  }

  public DatabaseOperationTask(
      JobsClient jobsClient, TablesClient tablesClient, DatabaseMetadata metadata) {
    super(jobsClient, tablesClient, metadata);
  }

  @Override
  protected boolean shouldRun() {
    return shouldRunTask() && !metadata.isMaintenanceJobDisabled(getType());
  }

  protected abstract boolean shouldRunTask();

  @Override
  protected List<String> getArgs() {
    return Arrays.asList("--databaseName", metadata.getDbName());
  }

  protected boolean launchJob() {
    String jobName = String.format("%s_%s", getType(), metadata.getDbName());
    jobId = jobsClient.launch(jobName, getType(), metadata.getCreator(), getArgs()).orElse(null);
    return jobId != null;
  }
}
