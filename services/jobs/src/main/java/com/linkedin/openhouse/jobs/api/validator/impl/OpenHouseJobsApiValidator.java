package com.linkedin.openhouse.jobs.api.validator.impl;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.*;

import com.linkedin.openhouse.common.api.validator.ApiValidatorUtil;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.api.validator.JobsApiValidator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OpenHouseJobsApiValidator implements JobsApiValidator {
  @Autowired private Validator validator;

  @Override
  public void validateCreateJob(CreateJobRequestBody createJobRequestBody) {
    List<String> validationFailures = new ArrayList<>();
    Set<ConstraintViolation<CreateJobRequestBody>> violationSet =
        validator.validate(createJobRequestBody);
    for (ConstraintViolation<CreateJobRequestBody> violation : violationSet) {
      validationFailures.add(
          String.format("%s : %s", ApiValidatorUtil.getField(violation), violation.getMessage()));
    }
    if (!createJobRequestBody.getJobName().matches(ALPHA_NUM_UNDERSCORE_REGEX_HYPHEN_ALLOW)) {
      validationFailures.add(
          String.format(
              "jobName : provided %s, %s",
              createJobRequestBody.getJobName(), ALPHA_NUM_UNDERSCORE_ERROR_MSG_HYPHEN_ALLOW));
    }
    if (!createJobRequestBody.getClusterId().matches(ALPHA_NUM_UNDERSCORE_REGEX_HYPHEN_ALLOW)) {
      validationFailures.add(
          String.format(
              "clusterId : provided %s, %s",
              createJobRequestBody.getClusterId(), ALPHA_NUM_UNDERSCORE_ERROR_MSG_HYPHEN_ALLOW));
    }
    final Map<String, String> executionConf = createJobRequestBody.getJobConf().getExecutionConf();
    if (!MapUtils.isEmpty(executionConf)
        && !JobConfValidator.executionConfigValidator(executionConf)) {
      validationFailures.add(
          String.format(
              "memory : provided %s, %s",
              executionConf.get(JOB_MEMORY_CONFIG), SPARK_MEMORY_FORMAT));
    }
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }

  // inner class to validate jobConf fields.
  private static class JobConfValidator {
    public static boolean executionConfigValidator(Map<String, String> executionConf) {
      String memoryConfig = executionConf.getOrDefault(JOB_MEMORY_CONFIG, "");
      return memoryConfig.matches(SPARK_MEMORY_FORMAT);
    }
  }
}
