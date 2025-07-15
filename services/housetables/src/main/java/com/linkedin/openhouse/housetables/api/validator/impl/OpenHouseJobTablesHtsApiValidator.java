package com.linkedin.openhouse.housetables.api.validator.impl;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.*;

import com.linkedin.openhouse.common.api.validator.ApiValidatorUtil;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.housetables.api.spec.model.Job;
import com.linkedin.openhouse.housetables.api.spec.model.JobKey;
import com.linkedin.openhouse.housetables.api.validator.HouseTablesApiValidator;
import java.util.ArrayList;
import java.util.List;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** Class implementing validations for all /hts/jobs/ REST endpoints. */
@Component
public class OpenHouseJobTablesHtsApiValidator implements HouseTablesApiValidator<JobKey, Job> {
  @Autowired private Validator validator;

  @Override
  public void validateGetEntity(JobKey key) {
    List<String> validationFailures = new ArrayList<>();
    if (!key.getJobId().matches(ALPHA_NUM_UNDERSCORE_REGEX_HYPHEN_ALLOW)) {
      validationFailures.add(
          String.format(
              "jobId provided: %s, %s",
              key.getJobId(), ALPHA_NUM_UNDERSCORE_ERROR_MSG_HYPHEN_ALLOW));
    }
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }

  @Override
  public void validateDeleteEntity(JobKey key) {
    // Same as validateGetEntity
    validateGetEntity(key);
  }

  @Override
  public void validateGetEntities(Job entity) {
    List<String> validationFailures = new ArrayList<>();

    if (entity.getJobId() != null
        && !entity.getJobId().matches(ALPHA_NUM_UNDERSCORE_REGEX_HYPHEN_ALLOW)) {
      validationFailures.add(
          String.format(
              "jobId  provided: %s, %s",
              entity.getJobId(), ALPHA_NUM_UNDERSCORE_ERROR_MSG_HYPHEN_ALLOW));
    }

    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }

  @Override
  public void validateGetEntities(Job entity, int page, int size, String sortBy) {
    throw new UnsupportedOperationException(
        "Pagination and sorting are not supported for Job entities");
  }

  @Override
  public void validatePutEntity(Job entity) {
    List<String> validationFailures = new ArrayList<>();

    for (ConstraintViolation<Job> violation : validator.validate(entity)) {
      validationFailures.add(
          String.format("%s : %s", ApiValidatorUtil.getField(violation), violation.getMessage()));
    }

    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
    // TODO: Add other validations for Job entity
  }

  @Override
  public void validateRenameEntity(JobKey fromKey, JobKey toKey) {
    // No rename operation for jobs
    throw new UnsupportedOperationException("Rename job is unsupported");
  }
}
