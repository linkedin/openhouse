package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.client.model.JobResponseBody;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.OtelConfig;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * A callable class to apply an operation to a table by running a Spark job. Takes care of the job
 * lifecycle using /jobs API.
 */
@Slf4j
@Getter
public abstract class TableOperationTask implements Callable<Optional<JobState>> {
  private static final long POLL_SLEEP_DURATION_MS = TimeUnit.MINUTES.toMillis(5);
  private static final long JOB_TIMEOUT_DURATION_MS = TimeUnit.HOURS.toMillis(3);
  private static final Meter METER = OtelConfig.getMeter(TableOperationTask.class.getName());

  @Getter(AccessLevel.NONE)
  protected final JobsClient jobsClient;

  @Getter(AccessLevel.NONE)
  protected final TablesClient tablesClient;

  protected final TableMetadata tableMetadata;

  @Getter(AccessLevel.NONE)
  private String jobId;

  protected TableOperationTask(
      JobsClient jobsClient, TablesClient tablesClient, TableMetadata tableMetadata) {
    this.jobsClient = jobsClient;
    this.tablesClient = tablesClient;
    this.tableMetadata = tableMetadata;
  }

  public abstract JobConf.JobTypeEnum getType();

  protected abstract List<String> getArgs();

  protected abstract boolean shouldRun();

  /* Returns empty value iff the callable was interrupted by future cancel. */
  @Override
  public Optional<JobState> call() {
    if (!shouldRun()) {
      log.info(
          "Skipping job for table {}, since the operation doesn't need to be run", tableMetadata);
      return Optional.empty();
    }
    log.info("Launching job for table {}", tableMetadata);
    Attributes typeTableAttributes =
        Attributes.of(
            AttributeKey.stringKey(AppConstants.TYPE), getType().getValue(),
            AttributeKey.stringKey(AppConstants.TABLE_NAME), tableMetadata.fqtn(),
            AttributeKey.stringKey(AppConstants.DATABASE_NAME), tableMetadata.getDbName());
    try {
      OtelConfig.executeWithStats(
          () -> {
            // this is a wrapper to convert boolean false to an exception
            if (!launchJob()) {
              throw new Exception();
            }
            return null;
          },
          METER,
          "submit",
          typeTableAttributes);
    } catch (Exception e) {
      log.error(
          "Could not launch job {} for table {}. Exception {}",
          getType(),
          tableMetadata,
          e.getMessage());
      return Optional.empty();
    }
    log.info("Launched a job with id {} for table {}", jobId, tableMetadata);
    long startTime = System.currentTimeMillis();
    while (!jobFinished()) {
      long elapsedTime = System.currentTimeMillis() - startTime;
      if (elapsedTime > JOB_TIMEOUT_DURATION_MS) {
        Optional<JobState> job = jobsClient.getState(jobId);
        // Do not cancel job if it is still in running state after 3 hours
        if (job.isPresent() && !job.get().equals(JobState.RUNNING)) {
          log.info(
              "Cancelling job: {} due to timeout for table: {} jobState: {}",
              getType(),
              tableMetadata,
              job.get());
          if (!jobsClient.cancelJob(jobId)) {
            log.error("Could not cancel job {} for table {}", getType(), tableMetadata);
            return Optional.empty();
          }
          break;
        }
      }
      try {
        Thread.sleep(POLL_SLEEP_DURATION_MS);
      } catch (InterruptedException e) {
        log.warn(
            String.format(
                "Interrupted status polling for job %s for table %s. Cancelling the job",
                getType(), tableMetadata),
            e);
        if (!jobsClient.cancelJob(jobId)) {
          log.error("Could not cancel job {} for table {}", getType(), tableMetadata);
          return Optional.empty();
        }
      }
    }
    Optional<JobResponseBody> ret = jobsClient.getJob(jobId);
    if (ret.isPresent()) {
      reportJobState(ret.get(), typeTableAttributes, startTime);
    } else {
      log.warn("Job: {} for table: {} has empty state", jobId, tableMetadata);
    }
    return Optional.of(Enum.valueOf(JobState.class, ret.get().getState().getValue()));
  }

  private void reportJobState(
      JobResponseBody jobResponse, Attributes tableAttributes, Long startTime) {
    // TODO: Cancelled job response needs to be verified. Values are in negative.
    log.info(
        "Finished job for table: {}, JobId {}, executionId {}, runTime {}, queuedTime {}, state {}",
        tableMetadata,
        jobResponse.getJobId(),
        jobResponse.getExecutionId(),
        jobResponse.getFinishTimeMs() - jobResponse.getCreationTimeMs(),
        jobResponse.getStartTimeMs() - jobResponse.getCreationTimeMs(),
        jobResponse.getState());
    JobResponseBody.StateEnum state = jobResponse.getState();
    Attributes attributes =
        tableAttributes
            .toBuilder()
            .put(AppConstants.STATUS, state.name())
            .put(AppConstants.JOB_ID, jobResponse.getJobId())
            .build();

    LongCounter jobCounter = METER.counterBuilder("job_count").build();
    METER
        .gaugeBuilder(AppConstants.RUN_DURATION_JOB)
        .ofLongs()
        .setUnit(TimeUnit.MILLISECONDS.toString())
        .buildWithCallback(
            measurement -> {
              measurement.record(System.currentTimeMillis() - startTime, attributes);
            });
    // report queued time for job
    if (jobResponse.getStartTimeMs() != 0) {
      METER
          .gaugeBuilder(AppConstants.QUEUED_TIME)
          .ofLongs()
          .setUnit(TimeUnit.MILLISECONDS.toString())
          .buildWithCallback(
              measurement -> {
                measurement.record(
                    jobResponse.getStartTimeMs() - jobResponse.getCreationTimeMs(), attributes);
              });
    }

    jobCounter.add(1, attributes);
    // TODO: histogram type metric below can be removed in favor of gauge type metric after all
    // jobs dashboards and
    // alerts have been migrated. jobcounter can also be removed since a counter for job status
    // has been added in
    // Jobs Scheduler
    LongHistogram jobRunDuration =
        METER
            .histogramBuilder(AppConstants.JOB_DURATION)
            .ofLongs()
            .setUnit(TimeUnit.MILLISECONDS.name())
            .build();
    jobRunDuration.record(System.currentTimeMillis() - startTime, attributes);
  }

  protected boolean launchJob() {
    String jobName =
        String.format(
            "%s_%s_%s", getType(), tableMetadata.getDbName(), tableMetadata.getTableName());
    jobId =
        jobsClient.launch(jobName, getType(), tableMetadata.getCreator(), getArgs()).orElse(null);
    return jobId != null;
  }

  protected boolean jobFinished() {
    return jobsClient.getState(jobId).map(JobState::isTerminal).orElse(false);
  }
}
