package com.linkedin.openhouse.jobs.spark.state;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.housetables.client.api.JobApi;
import com.linkedin.openhouse.housetables.client.model.CreateUpdateEntityRequestBodyJob;
import com.linkedin.openhouse.housetables.client.model.EntityResponseBodyJob;
import com.linkedin.openhouse.housetables.client.model.Job;
import com.linkedin.openhouse.jobs.util.RetryUtil;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;

/** Class providing methods to update job state and send heartbeats. */
@Slf4j
@AllArgsConstructor
public class StateManager {
  private static final int REQUEST_TIMEOUT_SECONDS = 30;
  private final RetryTemplate retryTemplate;
  private final JobApi jobsApi;

  public void sendHeartbeat(String jobId) {
    update(jobId, job -> job.setHeartbeatTimeMs(getCurrentTimeMillis()));
  }

  public void updateStartTime(String jobId) {
    update(jobId, job -> job.setStartTimeMs(getCurrentTimeMillis()));
  }

  public void updateFinishTime(String jobId) {
    update(jobId, job -> job.setFinishTimeMs(getCurrentTimeMillis()));
  }

  public void updateState(String jobId, JobState state) {
    update(jobId, job -> job.setState(state.name()));
  }

  private long getCurrentTimeMillis() {
    return Instant.now().toEpochMilli();
  }

  private void update(String jobId, Consumer<Job> jobModifier) {
    RetryUtil.executeWithRetry(
        retryTemplate,
        (RetryCallback<Void, Exception>)
            context -> {
              EntityResponseBodyJob response =
                  jobsApi.getJob(jobId).block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
              boolean success = false;
              if (response != null) {
                Job entity = response.getEntity();
                jobModifier.accept(entity);
                CreateUpdateEntityRequestBodyJob requestBody =
                    new CreateUpdateEntityRequestBodyJob();
                requestBody.setEntity(entity);
                response =
                    jobsApi.putJob(requestBody).block(Duration.ofSeconds(REQUEST_TIMEOUT_SECONDS));
                if (response != null) {
                  success = true;
                } else {
                  log.warn("Couldn't update job with id {}, no response from server", jobId);
                }
              } else {
                log.warn("Couldn't get job with id {}, no response from server", jobId);
              }
              if (!success) {
                throw new Exception(
                    String.format("Update operation failed for job with id %s", jobId));
              }
              return null;
            },
        null);
  }
}
