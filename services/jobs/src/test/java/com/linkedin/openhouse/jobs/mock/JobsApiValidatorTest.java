package com.linkedin.openhouse.jobs.mock;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.api.validator.JobsApiValidator;
import com.linkedin.openhouse.jobs.model.JobConf;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = CustomClusterPropertiesInitializer.class)
class JobsApiValidatorTest {

  @Autowired private JobsApiValidator jobsApiValidator;

  private CreateJobRequestBody makeJobRequestBodyFromJobNameClusterId(
      String jobName, String clusterId) {
    JobConf mockJobConf = Mockito.mock(JobConf.class);
    Mockito.when(mockJobConf.getMemory()).thenReturn("2G");
    return CreateJobRequestBody.builder()
        .jobName(jobName)
        .clusterId(clusterId)
        .jobConf(mockJobConf)
        .build();
  }

  private CreateJobRequestBody makeJobRequestBodyFromJobNameJobConf(String jobName, String memory) {
    return CreateJobRequestBody.builder()
        .jobName(jobName)
        .clusterId("clusterId")
        .jobConf(JobConf.builder().memory(memory).build())
        .build();
  }

  @Test
  public void validateJobRequestBody() {
    // Ensure hyphen is fine in clusterId and JobName
    assertDoesNotThrow(
        () ->
            jobsApiValidator.validateCreateJob(
                makeJobRequestBodyFromJobNameClusterId("job-name", "cluster-id")));

    assertDoesNotThrow(
        () ->
            jobsApiValidator.validateCreateJob(
                makeJobRequestBodyFromJobNameClusterId("complex_name-22", "complex-name_33")));

    assertDoesNotThrow(
        () ->
            jobsApiValidator.validateCreateJob(
                makeJobRequestBodyFromJobNameClusterId("nothing123", "crazy456")));
  }

  @Test
  public void testValidMemoryFormatInJobRequestBody() {
    // Ensure hyphen is fine in clusterId and JobName
    assertDoesNotThrow(
        () ->
            jobsApiValidator.validateCreateJob(
                makeJobRequestBodyFromJobNameJobConf("job-name", "4G")));

    assertDoesNotThrow(
        () ->
            jobsApiValidator.validateCreateJob(
                makeJobRequestBodyFromJobNameJobConf("job-name", "10G")));

    assertDoesNotThrow(
        () ->
            jobsApiValidator.validateCreateJob(
                makeJobRequestBodyFromJobNameJobConf("job-name", "256M")));
  }

  @Test
  public void testInValidMemoryFormatInJobRequestBody() {
    // Ensure hyphen is fine in clusterId and JobName
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            jobsApiValidator.validateCreateJob(
                makeJobRequestBodyFromJobNameJobConf("job-name", "-10G")));

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            jobsApiValidator.validateCreateJob(
                makeJobRequestBodyFromJobNameJobConf("job-name", "10P")));

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            jobsApiValidator.validateCreateJob(
                makeJobRequestBodyFromJobNameJobConf("job-name", "0G")));

    assertThrows(
        RequestValidationFailureException.class,
        () ->
            jobsApiValidator.validateCreateJob(
                makeJobRequestBodyFromJobNameJobConf("job-name", "G")));
  }
}
