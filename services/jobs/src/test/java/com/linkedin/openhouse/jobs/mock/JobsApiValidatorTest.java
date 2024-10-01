package com.linkedin.openhouse.jobs.mock;

import static com.linkedin.openhouse.jobs.model.JobConf.EXECUTION_CONF_KEY_PREFIX;
import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.api.validator.JobsApiValidator;
import com.linkedin.openhouse.jobs.model.JobConf;
import java.util.HashMap;
import java.util.Map;
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
    Mockito.when(mockJobConf.getExecutionConf()).thenReturn(new HashMap<>());
    return CreateJobRequestBody.builder()
        .jobName(jobName)
        .clusterId(clusterId)
        .jobConf(mockJobConf)
        .build();
  }

  private CreateJobRequestBody makeJobRequestBodyFromJobNameJobConf(String jobName, String prefix) {
    Map<String, String> executionConf = new HashMap<>();
    executionConf.put(prefix + "driver.memory", "10G");
    return CreateJobRequestBody.builder()
        .jobName(jobName)
        .clusterId("clusterId")
        .jobConf(JobConf.builder().executionConf(executionConf).build())
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
  public void testValidExecutionConfInJobRequestBody() {
    assertThrows(
        RequestValidationFailureException.class,
        () ->
            jobsApiValidator.validateCreateJob(
                makeJobRequestBodyFromJobNameJobConf("job-name", "")));
    assertDoesNotThrow(
        () ->
            jobsApiValidator.validateCreateJob(
                makeJobRequestBodyFromJobNameJobConf("job-name", EXECUTION_CONF_KEY_PREFIX)));
  }
}
