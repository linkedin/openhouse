package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * A callable class to apply an operation to a table by running a Spark job. Takes care of the job
 * lifecycle using /jobs API.
 */
@Slf4j
@Getter
public abstract class TableOperationTask extends OperationTask<TableMetadata> {

  protected TableOperationTask(
      JobsClient jobsClient, TablesClient tablesClient, TableMetadata tableMetadata) {
    super(jobsClient, tablesClient, tableMetadata);
  }

  protected boolean launchJob() {
    String jobName =
        String.format(
            "%s_%s_%s", getType(), getMetadata().getDbName(), getMetadata().getTableName());
    jobId =
        jobsClient.launch(jobName, getType(), getMetadata().getCreator(), getArgs()).orElse(null);
    return jobId != null;
  }
}
