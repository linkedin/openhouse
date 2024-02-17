package com.linkedin.openhouse.jobs.api.validator;

import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;

public interface JobsApiValidator {
  /**
   * Function to validate Job create request body.
   *
   * @param createJobRequestBody
   * @throws com.linkedin.openhouse.common.exception.RequestValidationFailureException if the
   *     request is invalid.
   */
  void validateCreateJob(CreateJobRequestBody createJobRequestBody);
}
