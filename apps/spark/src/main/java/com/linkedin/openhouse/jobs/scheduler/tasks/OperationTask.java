package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.client.model.JobResponseBody;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.Metadata;
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
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * A callable class to apply an operation to some entity (table/database) by running a Spark job.
 * Takes care of the job lifecycle using /jobs API.
 */
@Slf4j
@Getter
public abstract class OperationTask<T extends Metadata> implements Callable<Optional<JobState>> {
  public static final long POLL_INTERVAL_MS_DEFAULT = TimeUnit.MINUTES.toMillis(5);
  public static final long TIMEOUT_MS_DEFAULT = TimeUnit.HOURS.toMillis(3);
  private static final Meter METER = OtelConfig.getMeter(OperationTask.class.getName());

  @Getter(AccessLevel.NONE)
  protected final JobsClient jobsClient;

  @Getter(AccessLevel.NONE)
  protected final TablesClient tablesClient;

  @Getter(AccessLevel.NONE)
  protected final T metadata;

  @Getter(AccessLevel.NONE)
  private final long pollIntervalMs;

  @Getter(AccessLevel.NONE)
  private final long timeoutMs;

  @Setter(AccessLevel.PACKAGE)
  @Getter(AccessLevel.NONE)
  protected String jobId;

  @Setter(AccessLevel.PACKAGE)
  @Getter(AccessLevel.NONE)
  protected JobInfoManager jobInfoManager;

  @Setter(AccessLevel.PACKAGE)
  @Getter(AccessLevel.NONE)
  protected OperationMode operationMode;

  protected OperationTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      T metadata,
      long pollIntervalMs,
      long timeoutMs) {
    this.jobsClient = jobsClient;
    this.tablesClient = tablesClient;
    this.metadata = metadata;
    this.pollIntervalMs = pollIntervalMs;
    this.timeoutMs = timeoutMs;
  }

  protected OperationTask(JobsClient jobsClient, TablesClient tablesClient, T metadata) {
    this(jobsClient, tablesClient, metadata, POLL_INTERVAL_MS_DEFAULT, TIMEOUT_MS_DEFAULT);
  }

  public abstract JobConf.JobTypeEnum getType();

  protected abstract List<String> getArgs();

  protected abstract boolean shouldRun();

  /* Returns empty value iff the callable was interrupted by future cancel. */
  @Override
  public Optional<JobState> call() {
    Attributes typeAttributes =
        Attributes.of(
            AttributeKey.stringKey(AppConstants.TYPE),
            getType().getValue(),
            (metadata.getClass().equals(TableMetadata.class)
                ? AttributeKey.stringKey(AppConstants.TABLE_NAME)
                : AttributeKey.stringKey(AppConstants.DATABASE_NAME)),
            metadata.getEntityName());
    switch (operationMode) {
      case SUBMIT:
        Optional<JobState> submitJobState = submitJob(typeAttributes);
        moveJobToSubmittedStage();
        return submitJobState;
      case POLL:
        Optional<JobState> pollJobState = pollJobStatus(typeAttributes);
        moveJobToCompletedStage();
        return pollJobState;
      case SINGLE:
      default:
        submitJob(typeAttributes);
        return pollJobStatus(typeAttributes);
    }
  }

  private Optional<JobState> submitJob(Attributes typeAttributes) {
    if (!shouldRun()) {
      log.info("Skipping job for {}, since the operation doesn't need to be run", metadata);
      return Optional.empty();
    }
    log.info("Launching job for {}", metadata);
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
          typeAttributes);
    } catch (Exception e) {
      log.error(
          "Could not launch job {} for {}. Exception {}", getType(), metadata, e.getMessage());
      return Optional.empty();
    }
    log.info("Launched a job with id {} for {}", jobId, metadata);
    return Optional.of(JobState.SUBMITTED);
  }

  private Optional<JobState> pollJobStatus(Attributes typeAttributes) {
    long startTime = System.currentTimeMillis();
    while (!Thread.currentThread().isInterrupted() && !jobFinished()) {
      try {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (elapsedTime > timeoutMs) {
          Optional<JobState> job = jobsClient.getState(jobId);
          // Do not cancel job if it is still in running state after 3 hours
          if (job.isPresent() && !job.get().equals(JobState.RUNNING)) {
            log.info(
                "Cancelling job: {} due to timeout for {}: jobState: {}",
                getType(),
                metadata,
                job.get());
            if (!jobsClient.cancelJob(jobId)) {
              log.error("Could not cancel job {} for {}", getType(), metadata);
              return Optional.empty();
            }
            break;
          }
        }
        // Sleep for specified poll interval
        Thread.sleep(pollIntervalMs);
      } catch (InterruptedException e) {
        log.warn(
            String.format(
                "Interrupted status polling for job %s for %s. Cancelling the job",
                getType(), metadata),
            e);
        if (!jobsClient.cancelJob(jobId)) {
          log.error("Could not cancel job {} for {}", getType(), metadata);
          return Optional.empty();
        }
        // Interrupt the current thread and exit the loop
        Thread.currentThread().interrupt();
        break;
      }
    }
    Optional<JobResponseBody> ret = jobsClient.getJob(jobId);
    if (ret.isPresent()) {
      log.info(
          "Job: {} for {} has reached terminal state: {}", jobId, metadata, ret.get().getState());
      reportJobState(ret.get(), typeAttributes, startTime);
    } else {
      log.warn("Job: {} for {} has empty state", jobId, metadata);
    }
    return Optional.of(Enum.valueOf(JobState.class, ret.get().getState().getValue()));
  }

  private void moveJobToSubmittedStage() {
    try {
      if (jobId != null) {
        jobInfoManager.addData(new JobInfo(metadata, jobId));
      }
    } catch (InterruptedException e) {
      log.warn(
          "Interrupted while putting job to submitted job queue for metadata: {} and jobId: {}",
          metadata,
          jobId,
          e);
    }
  }

  private void moveJobToCompletedStage() {
    jobInfoManager.moveJobToCompletedStage(jobId);
  }

  private void reportJobState(
      JobResponseBody jobResponse, Attributes tableAttributes, Long startTime) {
    // TODO: Cancelled job response needs to be verified. Values are in negative.
    log.info(
        "Finished job for entity {}: JobId {}, executionId {}, runTime {}, queuedTime {}, state {}",
        metadata,
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

  protected abstract boolean launchJob();

  protected boolean jobFinished() {
    return jobsClient.getState(jobId).map(JobState::isTerminal).orElse(false);
  }

  @Override
  public String toString() {
    return String.format(
        "%s(jobId: %s, metadata: %s)", getClass().getSimpleName(), jobId, metadata);
  }
}
