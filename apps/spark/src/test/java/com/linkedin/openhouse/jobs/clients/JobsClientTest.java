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
  private static final String testClusterId = "test_cluster_id";
  private static final String testJobName = "test_job";
  private static final String testJobId = "test_job_id";
  private static final String testProxyUser = "test_proxy_user";
  private static final JobConf.JobTypeEnum testJobType = JobConf.JobTypeEnum.RETENTION;
  private final JobsClientFactory clientFactory = new JobsClientFactory("base_path", testClusterId);
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
    responseBody.setJobId(testJobId);
    Mono<JobResponseBody> responseMock = (Mono<JobResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(Mockito.any(Duration.class))).thenReturn(responseBody);
    Mockito.when(apiMock.createJob(Mockito.any(CreateJobRequestBody.class)))
        .thenReturn(responseMock);

    List<String> testArgs = Arrays.asList("-t", "test_table");
    Assertions.assertEquals(
        testJobId, client.launch(testJobName, testJobType, testProxyUser, testArgs).orElse(null));

    ArgumentCaptor<CreateJobRequestBody> argumentCaptor =
        ArgumentCaptor.forClass(CreateJobRequestBody.class);
    Mockito.verify(apiMock, Mockito.times(1)).createJob(argumentCaptor.capture());
    CreateJobRequestBody actualRequest = argumentCaptor.getValue();
    Assertions.assertEquals(testClusterId, actualRequest.getClusterId());
    Assertions.assertEquals(testJobName, actualRequest.getJobName());
    Assertions.assertNotNull(actualRequest.getJobConf());
    Assertions.assertEquals(testArgs, actualRequest.getJobConf().getArgs());
    Assertions.assertEquals(JobConf.JobTypeEnum.RETENTION, actualRequest.getJobConf().getJobType());

    // check no response
    Mockito.when(responseMock.block(Mockito.any(Duration.class))).thenReturn(null);
    Mockito.when(apiMock.createJob(Mockito.any(CreateJobRequestBody.class)))
        .thenReturn(responseMock);
    Assertions.assertFalse(
        client.launch(testJobName, testJobType, testProxyUser, testArgs).isPresent());
  }

  @Test
  void testGetState() {
    // check running
    JobResponseBody responseBody = new JobResponseBody();
    responseBody.setState(JobResponseBody.StateEnum.RUNNING);
    Mono<JobResponseBody> responseMock = (Mono<JobResponseBody>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(Mockito.any(Duration.class))).thenReturn(responseBody);
    Mockito.when(apiMock.getJob(testJobId)).thenReturn(responseMock);
    Assertions.assertEquals(JobState.RUNNING, client.getState(testJobId).orElse(null));

    // check succeeded
    responseBody = new JobResponseBody();
    responseBody.setState(JobResponseBody.StateEnum.SUCCEEDED);
    Mockito.when(responseMock.block(Mockito.any(Duration.class))).thenReturn(responseBody);
    Mockito.when(apiMock.getJob(testJobId)).thenReturn(responseMock);
    Assertions.assertEquals(JobState.SUCCEEDED, client.getState(testJobId).orElse(null));

    // check no response
    Mockito.when(responseMock.block(Mockito.any(Duration.class))).thenReturn(null);
    Mockito.when(apiMock.getJob(testJobId)).thenReturn(responseMock);
    Assertions.assertFalse(client.getState(testJobId).isPresent());
  }

  @Test
  void testCancel() {
    // check success
    Mono<Void> responseMock = (Mono<Void>) Mockito.mock(Mono.class);
    Mockito.when(responseMock.block(Mockito.any(Duration.class))).thenReturn(null);
    Mockito.when(apiMock.cancelJob(testJobId)).thenReturn(responseMock);
    Assertions.assertTrue(client.cancelJob(testJobId));

    // check failure
    Mockito.when(apiMock.cancelJob(testJobId)).thenThrow(WebClientResponseException.class);
    Assertions.assertFalse(client.cancelJob(testJobId));
  }
}
