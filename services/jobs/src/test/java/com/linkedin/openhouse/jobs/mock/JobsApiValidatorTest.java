package com.linkedin.openhouse.jobs.mock;

import static org.junit.jupiter.api.Assertions.*;

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
    return CreateJobRequestBody.builder()
        .jobName(jobName)
        .clusterId(clusterId)
        .jobConf(Mockito.mock(JobConf.class))
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
}
