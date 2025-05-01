package com.linkedin.openhouse.jobs.services;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.common.exception.JobStateConflictException;
import com.linkedin.openhouse.common.exception.NoSuchJobException;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.config.JobLaunchConf;
import com.linkedin.openhouse.jobs.dto.mapper.JobsMapper;
import com.linkedin.openhouse.jobs.model.JobDto;
import com.linkedin.openhouse.jobs.model.JobDtoPrimaryKey;
import com.linkedin.openhouse.jobs.repository.JobsInternalRepository;
import java.time.Instant;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class JobsServiceImpl implements JobsService {
  @Autowired JobsInternalRepository repository;
  @Autowired JobsMapper mapper;
  @Autowired JobsCoordinatorManager jobsCoordinatorManager;
  @Autowired JobsRegistry jobsRegistry;
  private static final MetricsReporter METRICS_REPORTER =
      MetricsReporter.of(MetricsConstant.JOBS_SERVICE);

  @Override
  public JobDto get(String jobId) {
    METRICS_REPORTER.count(
        MetricsConstant.REQUEST_COUNT, MetricsConstant.ACTION_TAG, MetricsConstant.GET);
    return repository
        .findById(JobDtoPrimaryKey.builder().jobId(jobId).build())
        .orElseThrow(() -> new NoSuchJobException(jobId));
  }

  @Override
  public JobDto create(CreateJobRequestBody createJobRequestBody) {
    METRICS_REPORTER.count(
        MetricsConstant.REQUEST_COUNT, MetricsConstant.ACTION_TAG, MetricsConstant.CREATE);
    String jobId = generateJobId(createJobRequestBody.getJobName());
    long timestamp = System.currentTimeMillis();
    // save job entry before submitting to capture the event
    // and ensure that entry exists before the job starts sending heartbeats
    JobLaunchConf jobLaunchConf =
        jobsRegistry.createLaunchConf(jobId, createJobRequestBody.getJobConf());
    JobDto jobDto =
        JobDto.builder()
            .jobId(jobId)
            .creationTimeMs(timestamp)
            .lastUpdateTimeMs(timestamp)
            .state(JobState.QUEUED)
            .engineType(jobLaunchConf.getEngineType())
            .retentionTimeSec(Instant.now().getEpochSecond())
            .build();
    jobDto = mapper.toJobDto(jobDto, createJobRequestBody);
    repository.save(jobDto);

    HouseJobHandle handle = jobsCoordinatorManager.submit(jobLaunchConf);
    JobInfo jobInfo = handle.getInfo();
    jobDto = jobDto.toBuilder().executionId(jobInfo.getExecutionId()).build();
    log.info("Submitted job: {}", jobDto);
    return repository.save(jobDto);
  }

  @Override
  public void cancel(String jobId) {
    METRICS_REPORTER.count(
        MetricsConstant.REQUEST_COUNT, MetricsConstant.ACTION_TAG, MetricsConstant.CANCEL);
    JobDto job =
        repository
            .findById(JobDtoPrimaryKey.builder().jobId(jobId).build())
            .orElseThrow(() -> new NoSuchJobException(jobId));
    checkCancellable(job);
    if (shouldCancel(job)) {
      HouseJobHandle handle =
          jobsCoordinatorManager.obtainHandle(job.getEngineType(), job.getExecutionId());
      handle.cancel();
      repository.save(job.toBuilder().state(JobState.CANCELLED).build());
    }
  }

  private void checkCancellable(JobDto job) {
    if (job.getState().isTerminal() && JobState.CANCELLED != job.getState()) {
      throw new JobStateConflictException(job.getState().name(), JobState.CANCELLED.name());
    }
  }

  private boolean shouldCancel(JobDto job) {
    return !job.getState().isTerminal();
  }

  private String generateJobId(String jobName) {
    return jobName + "_" + UUID.randomUUID();
  }
}
