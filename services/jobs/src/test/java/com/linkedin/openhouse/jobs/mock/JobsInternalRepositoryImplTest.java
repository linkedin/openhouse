package com.linkedin.openhouse.jobs.mock;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.housetables.client.api.JobApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import com.linkedin.openhouse.housetables.client.model.EntityResponseBodyJob;
import com.linkedin.openhouse.housetables.client.model.GetAllEntityResponseBodyJob;
import com.linkedin.openhouse.housetables.client.model.Job;
import com.linkedin.openhouse.internal.catalog.repository.CustomRetryListener;
import com.linkedin.openhouse.internal.catalog.repository.HtsRetryUtils;
import com.linkedin.openhouse.jobs.model.JobDto;
import com.linkedin.openhouse.jobs.model.JobDtoPrimaryKey;
import com.linkedin.openhouse.jobs.repository.JobsInternalRepository;
import com.linkedin.openhouse.jobs.repository.JobsInternalRepositoryImpl;
import com.linkedin.openhouse.jobs.repository.exception.JobsTableCallerException;
import com.linkedin.openhouse.jobs.repository.exception.JobsTableConcurrentUpdateException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@SpringBootTest
@ContextConfiguration(initializers = CustomClusterPropertiesInitializer.class)
public class JobsInternalRepositoryImplTest {

  @Autowired JobsInternalRepository jobsInternalRepository;

  private static final JobDto TEST_JOB =
      JobDto.builder()
          .jobId("jobId")
          .state(JobState.RUNNING)
          .jobName("jobName")
          .clusterId("clusterId")
          .executionId("exid")
          .build();

  @TestConfiguration
  public static class MockWebServerConfiguration {

    @Primary
    @Bean
    public JobApi provideMockHtsApiInstance() {
      // Routing the client to access port from Mock server so that Mock server can respond with
      // stub response.
      ApiClient apiClient = new ApiClient();
      String baseUrl = String.format("http://localhost:%s", mockHtsServer.getPort());
      apiClient.setBasePath(baseUrl);
      return new JobApi(apiClient);
    }
  }

  private static MockWebServer mockHtsServer;

  @BeforeAll
  static void setUp() throws IOException {
    mockHtsServer = new MockWebServer();
    mockHtsServer.start();
  }

  @AfterAll
  static void tearDown() throws IOException {
    mockHtsServer.shutdown();
  }

  @Test
  public void testRepoFindById() {
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody((new Gson()).toJson(mockResponseBody(TEST_JOB, "v1")))
            .addHeader("Content-Type", "application/json"));

    JobDto result =
        jobsInternalRepository
            .findById(JobDtoPrimaryKey.builder().jobId(TEST_JOB.getJobId()).build())
            .get();

    Assertions.assertEquals(result.getJobId(), TEST_JOB.getJobId());
    Assertions.assertEquals(result.getState(), TEST_JOB.getState());
  }

  @Test
  public void testRepoSaveNewRow() {
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(404)
            .setBody("RESPONSE BODY DOESNT MATTER")
            .addHeader("Content-Type", "application/json"));
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(201)
            .setBody((new Gson()).toJson(mockResponseBody(TEST_JOB, "v1")))
            .addHeader("Content-Type", "application/json"));

    JobDto result = jobsInternalRepository.save(TEST_JOB);
    Assertions.assertEquals(result.getJobId(), TEST_JOB.getJobId());
    Assertions.assertEquals(result.getState(), TEST_JOB.getState());
  }

  @Test
  public void testRepoSaveWithError() {
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(201)
            .setBody((new Gson()).toJson(mockResponseBody(TEST_JOB, "v1")))
            .addHeader("Content-Type", "application/json"));
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(409)
            .setBody("RESPONSE BODY DOESNT MATTER")
            .addHeader("Content-Type", "application/json"));
    Assertions.assertThrowsExactly(
        JobsTableConcurrentUpdateException.class, () -> jobsInternalRepository.save(TEST_JOB));

    // Surface unexpected errors
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(400)
            .setBody("RESPONSE BODY DOESNT MATTER")
            .addHeader("Content-Type", "application/json"));
    Assertions.assertThrowsExactly(
        WebClientResponseException.BadRequest.class, () -> jobsInternalRepository.save(TEST_JOB));
  }

  @Test
  public void testFindByIdNotFound() {
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(404)
            .setBody("")
            .addHeader("Content-Type", "application/json"));
    Assertions.assertFalse(
        jobsInternalRepository
            .findById(JobDtoPrimaryKey.builder().jobId("does_not_exist_id").build())
            .isPresent());
  }

  @Test
  public void testRepoDelete() {
    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class, () -> jobsInternalRepository.delete(TEST_JOB));
  }

  @Test
  public void testFindAll() {
    List<Job> jobs = new ArrayList<>();
    jobs.add(mockResponseBody(TEST_JOB, "v1").getEntity());
    jobs.add(
        mockResponseBody(TEST_JOB.toBuilder().jobId("jobId2").state(JobState.FAILED).build(), "v2")
            .getEntity());
    GetAllEntityResponseBodyJob listResponse = new GetAllEntityResponseBodyJob();

    /**
     * Need to use the reflection trick to help initializing the object with generated class {@link
     * GetAllUserTablesResponseBody}, which somehow doesn't provided proper setter in the generated
     * code.
     */
    Field resultField = ReflectionUtils.findField(GetAllEntityResponseBodyJob.class, "results");
    Assertions.assertNotNull(resultField);
    ReflectionUtils.makeAccessible(resultField);
    ReflectionUtils.setField(resultField, listResponse, jobs);

    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody((new Gson()).toJson(listResponse))
            .addHeader("Content-Type", "application/json"));
    List<JobDto> returnList = ImmutableList.copyOf(jobsInternalRepository.findAll());
    Assertions.assertEquals(returnList.size(), 2);
  }

  @Test
  public void testListWithEmptyResult() {
    GetAllEntityResponseBodyJob listResponse = new GetAllEntityResponseBodyJob();
    Field resultField = ReflectionUtils.findField(GetAllEntityResponseBodyJob.class, "results");
    Assertions.assertNotNull(resultField);
    ReflectionUtils.makeAccessible(resultField);
    ReflectionUtils.setField(resultField, listResponse, new ArrayList<>());

    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody((new Gson()).toJson(listResponse))
            .addHeader("Content-Type", "application/json"));

    List<JobDto> returnList = ImmutableList.copyOf(jobsInternalRepository.findAll());
    Assertions.assertEquals(returnList.size(), 0);
  }

  private EntityResponseBodyJob mockResponseBody(JobDto jobDto, String version) {
    EntityResponseBodyJob response = new EntityResponseBodyJob();
    response.entity(
        new Job()
            .jobId(jobDto.getJobId())
            .state(jobDto.getState().name())
            .version(version)
            .jobName(jobDto.getJobName())
            .clusterId(jobDto.getClusterId())
            .executionId(jobDto.getExecutionId()));
    return response;
  }

  @Test
  public void testRetryForFindByIdJobsApiCall() {
    // Injecting a Gateway timeout and an internal server error, will be translated to retryable
    // error. In fact only 500, 502, 503, 504 are retryable based on
    // com.linkedin.openhouse.jobs.repository.JobsInternalRepositoryImpl.getHtsRetryTemplate AND
    // com.linkedin.openhouse.jobs.repository.JobsInternalRepositoryImpl.handleHtsHttpError
    // These are covered in the following two tests.
    // Then inject a non retryable error(409) which should terminate the retry attempt and exception
    // will be thrown directly.
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(504)
            .setBody("")
            .addHeader("Content-Type", "application/json"));
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(500)
            .setBody("")
            .addHeader("Content-Type", "application/json"));
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(409)
            .setBody("")
            .addHeader("Content-Type", "application/json"));

    CustomRetryListener retryListener = new CustomRetryListener();
    ((JobsInternalRepositoryImpl) jobsInternalRepository)
        .getHtsRetryTemplate()
        .registerListener(retryListener);
    Assertions.assertThrows(
        JobsTableConcurrentUpdateException.class,
        () ->
            jobsInternalRepository.findById(
                JobDtoPrimaryKey.builder().jobId(TEST_JOB.getJobId()).build()));
    int actualRetryCount = retryListener.getRetryCount();

    Assertions.assertEquals(actualRetryCount, HtsRetryUtils.MAX_RETRY_ATTEMPT);
  }

  @Test
  public void testRetryForSaveJobsApiCall() {
    // needed to getCurrentVersion success
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody("")
            .addHeader("Content-Type", "application/json"));
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(503)
            .setBody("")
            .addHeader("Content-Type", "application/json"));
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(502)
            .setBody("")
            .addHeader("Content-Type", "application/json"));
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(400)
            .setBody("")
            .addHeader("Content-Type", "application/json"));

    CustomRetryListener retryListener = new CustomRetryListener();
    ((JobsInternalRepositoryImpl) jobsInternalRepository)
        .getHtsRetryTemplate()
        .registerListener(retryListener);

    Assertions.assertThrows(
        JobsTableCallerException.class, () -> jobsInternalRepository.save(TEST_JOB));
    int actualRetryCount = retryListener.getRetryCount();
    Assertions.assertEquals(actualRetryCount, HtsRetryUtils.MAX_RETRY_ATTEMPT);
  }

  @Test
  public void testRetryForHtsFindByIdCallOnConcurrentException() {
    mockHtsServer.enqueue(
        new MockResponse()
            .setResponseCode(409)
            .setBody("")
            .addHeader("Content-Type", "application/json"));

    CustomRetryListener retryListener = new CustomRetryListener();
    ((JobsInternalRepositoryImpl) jobsInternalRepository)
        .getHtsRetryTemplate()
        .registerListener(retryListener);

    Assertions.assertThrows(
        JobsTableConcurrentUpdateException.class,
        () ->
            jobsInternalRepository.findById(
                JobDtoPrimaryKey.builder().jobId(TEST_JOB.getJobId()).build()));
    int actualRetryCount = retryListener.getRetryCount();
    Assertions.assertEquals(actualRetryCount, 1);
  }
}
