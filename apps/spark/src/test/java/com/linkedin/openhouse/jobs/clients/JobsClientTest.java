package com.linkedin.openhouse.jobs.clients;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.JobsClientFactory;
import com.linkedin.openhouse.jobs.client.api.JobApi;
import com.linkedin.openhouse.jobs.client.model.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.client.model.JobResponseBody;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
public class JobsClientTest {
  private static final String TEST_CLUSTER_ID = "test_cluster_id";
  private static final String TEST_JOB_NAME = "test_job";
  private static final String TEST_JOB_ID = "test_job_id";
  private static final String TEST_PROXY_USER = "test_proxy_user";
  private static final JobConf.JobTypeEnum TEST_JOB_TYPE = JobConf.JobTypeEnum.RETENTION;
  private final JobsClientFactory clientFactory =
      new JobsClientFactory("base_path", TEST_CLUSTER_ID);
  private JobApi apiMock;
  private JobsClient client;

  @BeforeEach
  void setup() {
    apiMock = Mockito.mock(JobApi.class);
    RetryPolicy retryPolicy = new NeverRetryPolicy();
    client =
        clientFactory.create(RetryTemplate.builder().customPolicy(retryPolicy).build(), apiMock);
  }

  @Test
  void testLaunch() {
    // check success
    JobResponseBody responseBody = new JobResponseBody();
    responseBody.setJobId(TEST_JOB_ID);
    Mono<JobResponseBody> responseMock = (Mono<JobResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(Mockito.any(Duration.class))).thenReturn(responseBody);
    Mockito.when(apiMock.createJob(Mockito.any(CreateJobRequestBody.class)))
        .thenReturn(responseMock);

    List<String> testArgs = Arrays.asList("-t", "test_table");
    Assertions.assertEquals(
        TEST_JOB_ID,
        client.launch(TEST_JOB_NAME, TEST_JOB_TYPE, TEST_PROXY_USER, testArgs).orElse(null));

    ArgumentCaptor<CreateJobRequestBody> argumentCaptor =
        ArgumentCaptor.forClass(CreateJobRequestBody.class);
    Mockito.verify(apiMock, Mockito.times(1)).createJob(argumentCaptor.capture());
    CreateJobRequestBody actualRequest = argumentCaptor.getValue();
    Assertions.assertEquals(TEST_CLUSTER_ID, actualRequest.getClusterId());
    Assertions.assertEquals(TEST_JOB_NAME, actualRequest.getJobName());
    Assertions.assertNotNull(actualRequest.getJobConf());
    Assertions.assertEquals(testArgs, actualRequest.getJobConf().getArgs());
    Assertions.assertEquals(JobConf.JobTypeEnum.RETENTION, actualRequest.getJobConf().getJobType());

    // check no response
    Mockito.when(responseMock.block(Mockito.any(Duration.class))).thenReturn(null);
    Mockito.when(apiMock.createJob(Mockito.any(CreateJobRequestBody.class)))
        .thenReturn(responseMock);
    Assertions.assertFalse(
        client.launch(TEST_JOB_NAME, TEST_JOB_TYPE, TEST_PROXY_USER, testArgs).isPresent());
  }

  @Test
  void testGetState() {
    // check running
    JobResponseBody responseBody = new JobResponseBody();
    responseBody.setState(JobResponseBody.StateEnum.RUNNING);
    Mono<JobResponseBody> responseMock = (Mono<JobResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(Mockito.any(Duration.class))).thenReturn(responseBody);
    Mockito.when(apiMock.getJob(TEST_JOB_ID)).thenReturn(responseMock);
    Assertions.assertEquals(JobState.RUNNING, client.getState(TEST_JOB_ID).orElse(null));

    // check succeeded
    responseBody = new JobResponseBody();
    responseBody.setState(JobResponseBody.StateEnum.SUCCEEDED);
    Mockito.when(responseMock.block(Mockito.any(Duration.class))).thenReturn(responseBody);
    Mockito.when(apiMock.getJob(TEST_JOB_ID)).thenReturn(responseMock);
    Assertions.assertEquals(JobState.SUCCEEDED, client.getState(TEST_JOB_ID).orElse(null));

    // check no response
    Mockito.when(responseMock.block(Mockito.any(Duration.class))).thenReturn(null);
    Mockito.when(apiMock.getJob(TEST_JOB_ID)).thenReturn(responseMock);
    Assertions.assertFalse(client.getState(TEST_JOB_ID).isPresent());
  }

  @Test
  void testCancel() {
    // check success
    Mono<Void> responseMock = (Mono<Void>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(Mockito.any(Duration.class))).thenReturn(null);
    Mockito.when(apiMock.cancelJob(TEST_JOB_ID)).thenReturn(responseMock);
    Assertions.assertTrue(client.cancelJob(TEST_JOB_ID));

    // check failure
    Mockito.when(apiMock.cancelJob(TEST_JOB_ID)).thenThrow(WebClientResponseException.class);
    Assertions.assertFalse(client.cancelJob(TEST_JOB_ID));
  }
}
