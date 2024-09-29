package com.linkedin.openhouse.jobs.client;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.jobs.client.api.JobApi;
import com.linkedin.openhouse.jobs.client.model.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.client.model.JobResponseBody;
import com.linkedin.openhouse.jobs.util.RetryUtil;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;

/** A client for interacting with /jobs service. */
@Slf4j
@AllArgsConstructor
public class JobsClient {
  private static final int REQUEST_TIMEOUT_SECONDS = 20;
  private final RetryTemplate retryTemplate;
  private final JobApi api;
  private final String clusterId;

  /** Returns job id iff job launch was successful. */
  public Optional<String> launch(
      String jobName,
      JobConf.JobTypeEnum jobType,
      String proxyUser,
      Map<String, String> executionProperties,
      List<String> args) {
    final CreateJobRequestBody request = new CreateJobRequestBody();
    request.setClusterId(clusterId);
    request.setJobName(jobName);
    JobConf jobConf = new JobConf();
    jobConf.setJobType(jobType);
    jobConf.setProxyUser(proxyUser);
    jobConf.executionConf(executionProperties);
    jobConf.setArgs(args);
    request.setJobConf(jobConf);
    return Optional.ofNullable(
        RetryUtil.executeWithRetry(
            retryTemplate,
            (RetryCallback<String, Exception>)
                context -> {
                  JobResponseBody response =
                      api.createJob(request).block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
                  return response != null ? response.getJobId() : null;
                },
            null));
  }

  public Optional<String> launch(
      String jobName, JobConf.JobTypeEnum jobType, String proxyUser, List<String> args) {
    return launch(jobName, jobType, proxyUser, Collections.emptyMap(), args);
  }

  /** Returns (@link com.linkedin.openhouse.common.JobState) given job id. */
  public Optional<JobState> getState(String jobId) {
    return Optional.ofNullable(
        RetryUtil.executeWithRetry(
            retryTemplate,
            (RetryCallback<JobState, Exception>)
                context -> {
                  JobResponseBody response =
                      api.getJob(jobId).block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
                  return response != null
                      ? Enum.valueOf(JobState.class, response.getState().getValue())
                      : null;
                },
            null));
  }

  /** Returns (@link com.linkedin.openhouse.common.JobResponseBody) given job id. */
  public Optional<JobResponseBody> getJob(String jobId) {
    return Optional.ofNullable(
        RetryUtil.executeWithRetry(
            retryTemplate,
            (RetryCallback<JobResponseBody, Exception>)
                context -> api.getJob(jobId).block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS)),
            null));
  }

  /** Returns true iff cancellation was successful. */
  public boolean cancelJob(String jobId) {
    return RetryUtil.executeWithRetry(
        retryTemplate,
        (RetryCallback<Boolean, Exception>)
            context -> {
              api.cancelJob(jobId).block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
              return true;
            },
        false);
  }
}
