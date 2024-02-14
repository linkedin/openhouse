package com.linkedin.openhouse.jobs.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.api.spec.response.JobResponseBody;

/**
 * Interface layer between REST and Jobs backend. The implementation is injected into the Service
 * Controller.
 */
public interface JobsApiHandler {
  /**
   * Function to Get Job Resource for given jobId
   *
   * @param jobId
   * @return the job response body that would be returned to the client.
   */
  ApiResponse<JobResponseBody> get(String jobId);

  /**
   * Function to create a job resource.
   *
   * @param createJobRequestBody
   * @return the job response body that would be returned to the client.
   */
  ApiResponse<JobResponseBody> create(CreateJobRequestBody createJobRequestBody);

  /**
   * Function to cancel job with given jobId
   *
   * @param jobId
   * @return empty body on successful cancellation
   */
  ApiResponse<Void> cancel(String jobId);
}
