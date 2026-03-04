package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.google.common.base.Preconditions;
import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.client.model.JobResponseBody;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.Metadata;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
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
 *
 * <p>NOTE: Every implementation must implement a static {@code OPERATION_TYPE} field in order for
 * the job scheduler to load the OperationTask.
 */
@Slf4j
@Getter
public abstract class OperationTask<T extends Metadata> implements Callable<Optional<JobState>> {
  public static final long POLL_INTERVAL_MS_DEFAULT = TimeUnit.MINUTES.toMillis(5);
  public static final long QUEUED_TIMEOUT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(10);
  public static final long TASK_TIMEOUT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(15);
  private static final String METRICS_SCOPE = OperationTask.class.getName();

  @Getter(AccessLevel.NONE)
  protected final JobsClient jobsClient;

  @Getter(AccessLevel.NONE)
  protected final TablesClient tablesClient;

  @Getter(AccessLevel.NONE)
  protected final T metadata;

  @Getter(AccessLevel.NONE)
  private final long pollIntervalMs;

  @Getter(AccessLevel.NONE)
  private final long queuedTimeoutMs;

  @Getter(AccessLevel.NONE)
  private final long taskTimeoutMs; // should be larger than queuedTimeoutMs

  @Setter(AccessLevel.PACKAGE)
  @Getter(AccessLevel.PUBLIC)
  protected String jobId;

  @Setter(AccessLevel.PACKAGE)
  @Getter(AccessLevel.NONE)
  protected JobInfoManager jobInfoManager;

  @Setter(AccessLevel.PACKAGE)
  @Getter(AccessLevel.NONE)
  protected OperationMode operationMode;

  @Setter(AccessLevel.PACKAGE)
  @Getter(AccessLevel.NONE)
  protected OtelEmitter otelEmitter;

  protected OperationTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      T metadata,
      long pollIntervalMs,
      long queuedTimeoutMs,
      long taskTimeoutMs) {
    Preconditions.checkArgument(
        taskTimeoutMs > queuedTimeoutMs,
        String.format(
            "Task timeout must be larger than queued timeout: taskTimeoutMs=%s, queuedTimeoutMs=%s",
            taskTimeoutMs, queuedTimeoutMs));
    this.jobsClient = jobsClient;
    this.tablesClient = tablesClient;
    this.metadata = metadata;
    this.pollIntervalMs = pollIntervalMs;
    this.queuedTimeoutMs = queuedTimeoutMs;
    this.taskTimeoutMs = taskTimeoutMs;
  }

  protected OperationTask(JobsClient jobsClient, TablesClient tablesClient, T metadata) {
    this(
        jobsClient,
        tablesClient,
        metadata,
        POLL_INTERVAL_MS_DEFAULT,
        QUEUED_TIMEOUT_MS_DEFAULT,
        TASK_TIMEOUT_MS_DEFAULT);
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
    Optional<JobState> submitJobState;
    switch (operationMode) {
      case SUBMIT:
        submitJobState = submitJob(typeAttributes);
        // If job state is not empty then poll for status
        if (submitJobState.isPresent()) {
          moveJobToSubmittedStage();
        }
        return submitJobState;
      case POLL:
        Optional<JobState> pollJobState = pollJobStatus(typeAttributes);
        moveJobToCompletedStage();
        return pollJobState;
      case SINGLE:
      default:
        submitJobState = submitJob(typeAttributes);
        // If job state is not empty then poll for status
        if (submitJobState.isPresent()) {
          return pollJobStatus(typeAttributes);
        }
        return submitJobState;
    }
  }

  private Optional<JobState> submitJob(Attributes typeAttributes) {
    if (!shouldRun()) {
      log.info("Skipping job for {}, since the operation doesn't need to be run", metadata);
      return Optional.empty();
    }
    log.info("Launching job for {}", metadata);
    try {
      otelEmitter.executeWithStats(
          () -> {
            // this is a wrapper to convert boolean false to an exception
            if (!launchJob()) {
              throw new Exception();
            }
            return null;
          },
          METRICS_SCOPE,
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

  protected Optional<JobState> pollJobStatus(Attributes typeAttributes) {
    long startTime = System.currentTimeMillis();
    try {
      Optional<JobState> jobState;
      do {
        jobState = jobsClient.getState(jobId);
        long elapsedTime = System.currentTimeMillis() - startTime;
        // Exit status check if a job is queued for more than queuedTimeoutMs.
        if (elapsedTime > queuedTimeoutMs) {
          if (jobState.isPresent() && jobState.get().equals(JobState.QUEUED)) {
            log.info(
                "Exiting status check for {} for {} due to queued timeout", getType(), metadata);
            break;
          }
        }
        // Exit status check if a job is running for more than taskTimeoutMs.
        if (elapsedTime > taskTimeoutMs) {
          log.info("Exiting status check for {} for {} due to task timeout", getType(), metadata);
          break;
        }
        Thread.sleep(pollIntervalMs);
      } while (!jobFinished(jobState));
    } catch (InterruptedException e) {
      // Exit status check if scheduler send out an interrupt signal.
      log.warn(
          "Exiting status check, interrupted status polling for job {} for {}: ",
          getType(),
          metadata,
          e);
    }
    Optional<JobResponseBody> ret = jobsClient.getJob(jobId);
    if (ret.isPresent()) {
      reportJobState(ret.get(), typeAttributes, startTime);
    } else {
      log.warn("Job: {} for {} has empty state", jobId, metadata);
    }
    return Optional.of(
        Enum.valueOf(
            JobState.class,
            ret.map(JobResponseBody::getState)
                .map(JobResponseBody.StateEnum::getValue)
                .orElse(JobState.SKIPPED.toString())));
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

    otelEmitter.gauge(
        METRICS_SCOPE,
        AppConstants.RUN_DURATION_JOB,
        System.currentTimeMillis() - startTime,
        attributes);
    // report queued time for job
    if (jobResponse.getStartTimeMs() != 0) {
      otelEmitter.gauge(
          METRICS_SCOPE,
          AppConstants.QUEUED_TIME,
          jobResponse.getStartTimeMs() - jobResponse.getCreationTimeMs(),
          attributes);
    }
    otelEmitter.count(METRICS_SCOPE, "job_count", 1, attributes);
    // TODO: histogram type metric below can be removed in favor of gauge type metric after all
    // jobs dashboards and
    // alerts have been migrated. jobcounter can also be removed since a counter for job status
    // has been added in
    // Jobs Scheduler
    otelEmitter.time(
        METRICS_SCOPE,
        AppConstants.JOB_DURATION,
        System.currentTimeMillis() - startTime,
        attributes);

    // Granular attributes to publish entity level job metrics
    Attributes granularAttributes =
        Attributes.of(
            AttributeKey.stringKey(AppConstants.ENTITY_NAME),
            metadata.getEntityName(),
            AttributeKey.stringKey(AppConstants.ENTITY_TYPE),
            metadata.getClass().getSimpleName().replace("Metadata", ""),
            AttributeKey.stringKey(AppConstants.JOB_TYPE),
            getType().getValue(),
            AttributeKey.stringKey(AppConstants.JOB_STATE),
            state.name());

    otelEmitter.count(METRICS_SCOPE, "maintenance_job_completed", 1, granularAttributes);
  }

  protected abstract boolean launchJob();

  protected boolean jobFinished(Optional<JobState> job) {
    return job.map(JobState::isTerminal).orElse(false);
  }

  @Override
  public String toString() {
    return String.format(
        "%s(jobId: %s, metadata: %s)", getClass().getSimpleName(), jobId, metadata);
  }
}
