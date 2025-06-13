package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * A callable class to apply an operation to a table by running a Spark job. Takes care of the job
 * lifecycle using /jobs API.
 */
@Slf4j
@Getter
public abstract class TableOperationTask<T extends TableMetadata> extends OperationTask<T> {
  protected TableOperationTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      T metadata,
      long pollIntervalMs,
      long queuedTimeoutMs,
      long taskTimeoutMs) {
    super(jobsClient, tablesClient, metadata, pollIntervalMs, queuedTimeoutMs, taskTimeoutMs);
  }

  protected TableOperationTask(JobsClient jobsClient, TablesClient tablesClient, T metadata) {
    super(jobsClient, tablesClient, metadata);
  }

  protected boolean launchJob() {
    String jobName =
        String.format("%s_%s_%s", getType(), metadata.getDbName(), metadata.getTableName());
    jobId =
        jobsClient
            .launch(
                jobName, getType(), metadata.getCreator(), getJobExecutionProperties(), getArgs())
            .orElse(null);
    return jobId != null;
  }

  private Map<String, String> getJobExecutionProperties() {
    final String propertyPrefix = getType().name() + ".";
    return metadata.getJobExecutionProperties().entrySet().stream()
        .filter(e -> e.getKey().startsWith(propertyPrefix))
        .map(
            e ->
                new AbstractMap.SimpleEntry<>(
                    e.getKey().substring(propertyPrefix.length()), e.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
