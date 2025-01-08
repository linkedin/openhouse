package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.OtelConfig;
import com.linkedin.openhouse.jobs.util.ReplicationConfig;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/** A task to apply replication to a table. */
@Slf4j
public class TableReplicationTask extends TableOperationTask<TableMetadata> {
  public static final JobConf.JobTypeEnum OPERATION_TYPE = JobConf.JobTypeEnum.REPLICATION;
  private static final Meter METER = OtelConfig.getMeter(OperationTask.class.getName());

  protected TableReplicationTask(
      JobsClient jobsClient, TablesClient tablesClient, TableMetadata tableMetadata) {
    super(jobsClient, tablesClient, tableMetadata);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected List<String> getArgs() {
    return null;
  }

  /* Returns empty value iff the callable was interrupted by future cancel. */
  @Override
  public Optional<JobState> call() {
    if (!shouldRun()) {
      log.info("Skipping job for {}, since the operation doesn't need to be run", metadata);
      return Optional.empty();
    }
    List<ReplicationConfig> replicationConfigs = metadata.getReplicationConfig();
    for (ReplicationConfig config : replicationConfigs) {
      log.info("Launching job for {}", metadata);
      Attributes typeAttributes =
          Attributes.of(
              AttributeKey.stringKey(AppConstants.TYPE),
              getType().getValue(),
              (metadata.getClass().equals(TableMetadata.class)
                  ? AttributeKey.stringKey(AppConstants.TABLE_NAME)
                  : AttributeKey.stringKey(AppConstants.DATABASE_NAME)),
              metadata.getEntityName());
      try {
        OtelConfig.executeWithStats(
            () -> {
              // this is a wrapper to convert boolean false to an exception
              if (!launchJob(config)) {
                throw new Exception();
              }
              return null;
            },
            METER,
            "submit",
            typeAttributes);
      } catch (Exception e) {
        log.error(
            "Could not launch job {} for {}. Exception {}", getType(), metadata, e.getMessage());
        return Optional.empty();
      }
      log.info("Launched a job for {}", metadata);
      // TODO: implement wait loop for job to finish and update metrics and job state
      // TODO: update the jobState with returned value from Airflow client
    }
    return Optional.of(Enum.valueOf(JobState.class, JobState.FAILED.name()));
  }

  protected boolean launchJob(ReplicationConfig config) {
    String jobName =
        String.format(
            "%s_%s_%s_%s",
            getType(), config.getCluster(), metadata.getDbName(), metadata.getTableName());
    // TODO: Trigger Airflow job using airflow job client. Config can be used to create airflow
    // client params
    // TODO: Poll for job ID
    log.info("Triggering Replication job: {} via airflow client", jobName);
    return false;
  }

  @Override
  protected boolean shouldRun() {
    return metadata.isPrimary() && metadata.getReplicationConfig() != null;
  }
}
