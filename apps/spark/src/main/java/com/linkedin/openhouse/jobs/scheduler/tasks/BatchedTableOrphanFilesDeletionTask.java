package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.jobs.util.TableMetadataBatch;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * A task to remove orphan files from a batch of tables in a single Spark job. Pairs with {@code
 * com.linkedin.openhouse.jobs.spark.BatchedOrphanFilesDeletionSparkApp} via the {@link
 * JobConf.JobTypeEnum#ORPHAN_FILES_DELETION_BATCH} JobType.
 *
 * <p>The legacy {@link com.linkedin.openhouse.jobs.scheduler.JobsScheduler} pre-dates the optimizer
 * service, so this task omits the optimizer-only CLI flags ({@code --resultsEndpoint}, {@code
 * --operationIds}, {@code --tableUuids}). The Spark app treats them as optional and falls back to
 * HTS-only lifecycle tracking when they are absent.
 *
 * @see <a href="https://iceberg.apache.org/docs/latest/maintenance/#delete-orphan-files">Delete
 *     orphan files</a>
 */
@Slf4j
@Getter
public class BatchedTableOrphanFilesDeletionTask extends OperationTask<TableMetadataBatch> {
  public static final JobConf.JobTypeEnum OPERATION_TYPE =
      JobConf.JobTypeEnum.ORPHAN_FILES_DELETION_BATCH;

  public BatchedTableOrphanFilesDeletionTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      TableMetadataBatch metadata,
      long pollIntervalMs,
      long queuedTimeoutMs,
      long taskTimeoutMs) {
    super(jobsClient, tablesClient, metadata, pollIntervalMs, queuedTimeoutMs, taskTimeoutMs);
  }

  public BatchedTableOrphanFilesDeletionTask(
      JobsClient jobsClient, TablesClient tablesClient, TableMetadataBatch metadata) {
    super(jobsClient, tablesClient, metadata);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected List<String> getArgs() {
    String tableNames =
        metadata.getTables().stream().map(TableMetadata::fqtn).collect(Collectors.joining(","));
    return Arrays.asList("--tableNames", tableNames);
  }

  @Override
  protected boolean shouldRun() {
    return !metadata.getTables().isEmpty();
  }

  @Override
  protected boolean launchJob() {
    String jobName =
        String.format("%s_%s_%d", getType(), metadata.getDbName(), metadata.getTables().size());
    Map<String, String> executionProperties = Collections.emptyMap();
    String proxyUser = metadata.getTables().get(0).getCreator();
    jobId =
        jobsClient
            .launch(jobName, getType(), proxyUser, executionProperties, getArgs())
            .orElse(null);
    return jobId != null;
  }
}
