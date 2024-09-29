package com.linkedin.openhouse.jobs.mock;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.api.spec.response.JobResponseBody;
import com.linkedin.openhouse.jobs.model.JobConf;

public final class RequestConstants {
  private RequestConstants() {}

  public static final JobResponseBody.JobResponseBodyBuilder TEST_GET_JOB_RESPONSE_BODY_BUILDER =
      JobResponseBody.builder()
          .jobName("my_job")
          .clusterId("my_cluster")
          .state(JobState.RUNNING)
          .jobConf(JobConf.builder().jobType(JobConf.JobType.RETENTION).build());

  public static final CreateJobRequestBody TEST_CREATE_JOB_REQUEST_BODY =
      CreateJobRequestBody.builder()
          .jobName("my_job")
          .clusterId("my_cluster")
          .jobConf(JobConf.builder().jobType(JobConf.JobType.RETENTION).build())
          .build();

  public static final CreateJobRequestBody TEST_CREATE_INVALID_JOB_REQUEST_BODY =
      CreateJobRequestBody.builder().jobName("my_job").clusterId("my_cluster").build();
}
