package com.linkedin.openhouse.jobs.mock;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.JobStateConflictException;
import com.linkedin.openhouse.common.exception.NoSuchJobException;
import com.linkedin.openhouse.jobs.api.handler.JobsApiHandler;
import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.api.spec.response.JobResponseBody;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
@Primary
public class MockJobsApiHandler implements JobsApiHandler {
  @Override
  public ApiResponse<JobResponseBody> get(String jobId) {
    JobResponseBody responseBody =
        RequestConstants.TEST_GET_JOB_RESPONSE_BODY_BUILDER.jobId(jobId).build();
    switch (jobId) {
      case "my_job_4xx":
        throw new NoSuchJobException(jobId);
      case "my_job_2xx":
        return ApiResponse.<JobResponseBody>builder()
            .httpStatus(HttpStatus.OK)
            .responseBody(responseBody)
            .build();
      default:
        return null;
    }
  }

  @Override
  public ApiResponse<JobResponseBody> create(CreateJobRequestBody createJobRequestBody) {
    if (createJobRequestBody.getJobConf() == null) {
      return ApiResponse.<JobResponseBody>builder().httpStatus(HttpStatus.BAD_REQUEST).build();
    }
    JobResponseBody responseBody =
        RequestConstants.TEST_GET_JOB_RESPONSE_BODY_BUILDER
            .jobName(createJobRequestBody.getJobName())
            .jobId(createJobRequestBody.getJobName() + "_id")
            .clusterId(createJobRequestBody.getClusterId())
            .jobConf(createJobRequestBody.getJobConf())
            .build();
    return ApiResponse.<JobResponseBody>builder()
        .httpStatus(HttpStatus.CREATED)
        .responseBody(responseBody)
        .build();
  }

  @Override
  public ApiResponse<Void> cancel(String jobId) {
    switch (jobId) {
      case "my_job_404":
        throw new NoSuchJobException(jobId);
      case "my_job_409":
        throw new JobStateConflictException(JobState.SUCCEEDED.name(), JobState.CANCELLED.name());
      case "my_job_2xx":
        return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
      default:
        return null;
    }
  }
}
