package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.exception.OperationTaskException;
import com.linkedin.openhouse.jobs.util.DirectoryMetadata;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * A callable class to apply an operation to a table directory by running a Spark job. Takes care of
 * the job lifecycle using /jobs API.
 */
@Slf4j
@Getter
public abstract class TableDirectoryOperationTask extends OperationTask<DirectoryMetadata> {
  protected TableDirectoryOperationTask(
      JobsClient jobsClient, TablesClient tablesClient, DirectoryMetadata directoryMetadata) {
    super(jobsClient, tablesClient, directoryMetadata);
  }

  protected boolean launchJob() {
    String jobName = String.format("%s_%s", getType(), getMetadata().getValue());
    try {
      jobId =
          jobsClient.launch(jobName, getType(), getMetadata().getCreator(), getArgs()).orElse(null);
    } catch (OperationTaskException e) {
      log.error("Failed to launch job: {}", e.getMessage());
      jobId = null;
    }
    return jobId != null;
  }
}
