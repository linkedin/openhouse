package com.linkedin.openhouse.jobs.api.validator;

import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;

public interface JobsApiValidator {
  /**
   * Function to validate Job create request body.
   *
   * @param createJobRequestBody
   * @throws {@link com.linkedin.openhouse.common.exception.RequestValidationFailureException}
   */
  void validateCreateJob(CreateJobRequestBody createJobRequestBody);
}
