package com.linkedin.openhouse.housetables.mock.api;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.housetables.api.spec.model.Job;
import com.linkedin.openhouse.housetables.api.spec.model.JobKey;
import com.linkedin.openhouse.housetables.api.validator.HouseTablesApiValidator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class OpenHouseJobTablesValidatorTest {
  @Autowired private HouseTablesApiValidator<JobKey, Job> jobTablesHtsApiValidator;

  @Test
  public void validateGetEntitySuccess() {
    JobKey jobKey = JobKey.builder().jobId("job1").build();
    assertDoesNotThrow(() -> jobTablesHtsApiValidator.validateGetEntity(jobKey));
  }

  @Test
  public void validateInValidGetTable() {
    JobKey jobKey = JobKey.builder().jobId("job?1").build();

    // Invalid tableId and empty databaseId
    assertThrows(
        RequestValidationFailureException.class,
        () -> jobTablesHtsApiValidator.validateGetEntity(jobKey));
  }

  @Test
  public void validateGetAllEntitiesSuccess() {
    Job job = Job.builder().jobId("job1").build();
    assertDoesNotThrow(() -> jobTablesHtsApiValidator.validateGetEntities(job));
  }

  @Test
  public void validateIncorrectGetAllEntities() {
    Job userTable = Job.builder().jobId("job??").build();

    // Invalid databaseId and empty tableId.
    assertThrows(
        RequestValidationFailureException.class,
        () -> jobTablesHtsApiValidator.validateGetEntities(userTable));
  }

  @Test
  public void validatePutEntitySuccess() {
    Job job = Job.builder().jobId("job1").jobName("validJob").clusterId("local").build();
    assertDoesNotThrow(() -> jobTablesHtsApiValidator.validatePutEntity(job));
  }

  @Test
  public void validateInvalidPutEntityRequest() {
    Job job = Job.builder().jobId("job1").clusterId("local").build();

    // Missing jobName
    assertThrows(
        RequestValidationFailureException.class,
        () -> jobTablesHtsApiValidator.validatePutEntity(job));

    Job modifiedJob = Job.builder().jobName("customJob").clusterId("local").build();

    // Missing jobId
    assertThrows(
        RequestValidationFailureException.class,
        () -> jobTablesHtsApiValidator.validatePutEntity(modifiedJob));
  }

  @Test
  public void validateDeleteEntitySuccess() {
    JobKey jobKey = JobKey.builder().jobId("job1").build();

    assertDoesNotThrow(() -> jobTablesHtsApiValidator.validateDeleteEntity(jobKey));
  }

  @Test
  public void validateDeleteEntitySpecialCharacter() {
    JobKey jobKey = JobKey.builder().jobId("job??").build();

    // Inadmissible values for jobId
    assertThrows(
        RequestValidationFailureException.class,
        () -> jobTablesHtsApiValidator.validateDeleteEntity(jobKey));
  }
}
