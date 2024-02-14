package com.linkedin.openhouse.jobs.spark.state;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.housetables.client.api.JobApi;
import com.linkedin.openhouse.housetables.client.model.EntityResponseBodyJob;
import com.linkedin.openhouse.housetables.client.model.Job;
import io.netty.handler.timeout.ReadTimeoutException;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
public class StateManagerTest {
  private static final String testJobId = "test_job_id";
  private JobApi apiMock;

  @BeforeEach
  void setup() {
    apiMock = Mockito.mock(JobApi.class);
  }

  @Test
  void testSuccess() {
    Job jobMock = Mockito.mock(Job.class);
    Mono<EntityResponseBodyJob> responseMock =
        (Mono<EntityResponseBodyJob>) Mockito.mock(Mono.class);
    EntityResponseBodyJob responseEntityMock = Mockito.mock(EntityResponseBodyJob.class);

    Mockito.when(responseEntityMock.getEntity()).thenReturn(jobMock);
    Mockito.when(responseMock.block(Mockito.any())).thenReturn(responseEntityMock);
    Mockito.when(apiMock.getJob(Mockito.refEq(testJobId))).thenReturn(responseMock);
    Mockito.when(apiMock.putJob(Mockito.any())).thenReturn(responseMock);

    Instant now = Instant.now();

    StateManager stateManager = createStateManager(0);
    try (MockedStatic<Instant> instant = Mockito.mockStatic(Instant.class)) {
      instant.when(Instant::now).thenReturn(now);
      stateManager.updateStartTime(testJobId);
      stateManager.updateFinishTime(testJobId);
      stateManager.sendHeartbeat(testJobId);
      stateManager.updateState(testJobId, JobState.RUNNING);
      Mockito.verify(jobMock).setStartTimeMs(now.toEpochMilli());
      Mockito.verify(jobMock).setFinishTimeMs(now.toEpochMilli());
      Mockito.verify(jobMock).setHeartbeatTimeMs(now.toEpochMilli());
      Mockito.verify(jobMock).setState(JobState.RUNNING.name());
    }
  }

  @Test
  void testGetJobFailure() {
    final int numAttempts = 3;
    Mono<EntityResponseBodyJob> responseMock =
        (Mono<EntityResponseBodyJob>) Mockito.mock(Mono.class);

    Mockito.when(responseMock.block(Mockito.any())).thenReturn(null);
    Mockito.when(apiMock.getJob(Mockito.refEq(testJobId))).thenReturn(responseMock);

    Instant now = Instant.now();
    StateManager stateManager = createStateManager(numAttempts);
    try (MockedStatic<Instant> instant = Mockito.mockStatic(Instant.class)) {
      instant.when(Instant::now).thenReturn(now);
      stateManager.updateStartTime(testJobId);
      stateManager.updateFinishTime(testJobId);
      stateManager.sendHeartbeat(testJobId);
      stateManager.updateState(testJobId, JobState.RUNNING);
      Mockito.verify(apiMock, Mockito.never()).putJob(Mockito.any());
      Mockito.verify(apiMock, Mockito.times(numAttempts * 4)).getJob(Mockito.eq(testJobId));
    }
  }

  @Test
  void testException() {
    final int numAttempts = 3;
    Mono<EntityResponseBodyJob> responseMock =
        (Mono<EntityResponseBodyJob>) Mockito.mock(Mono.class);

    Mockito.when(responseMock.block(Mockito.any())).thenThrow(ReadTimeoutException.class);
    Mockito.when(apiMock.getJob(Mockito.refEq(testJobId))).thenReturn(responseMock);

    Instant now = Instant.now();

    StateManager stateManager = createStateManager(numAttempts);
    try (MockedStatic<Instant> instant = Mockito.mockStatic(Instant.class)) {
      instant.when(Instant::now).thenReturn(now);
      stateManager.updateStartTime(testJobId);
      stateManager.updateFinishTime(testJobId);
      stateManager.sendHeartbeat(testJobId);
      stateManager.updateState(testJobId, JobState.RUNNING);
      Mockito.verify(apiMock, Mockito.never()).putJob(Mockito.any());
      Mockito.verify(apiMock, Mockito.times(numAttempts * 4)).getJob(Mockito.eq(testJobId));
    }
  }

  private StateManager createStateManager(int numAttempts) {
    RetryPolicy retryPolicy = new NeverRetryPolicy();
    if (numAttempts > 0) {
      retryPolicy = new SimpleRetryPolicy(numAttempts);
    }
    return new StateManager(RetryTemplate.builder().customPolicy(retryPolicy).build(), apiMock);
  }
}
