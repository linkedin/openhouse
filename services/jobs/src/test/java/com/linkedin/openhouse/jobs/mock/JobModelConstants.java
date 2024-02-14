package com.linkedin.openhouse.jobs.mock;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.jobs.model.JobConf;
import com.linkedin.openhouse.jobs.model.JobDto;

public class JobModelConstants {
  static final JobDto JOB_DTO = getFullJobDto();

  static JobDto.JobDtoBuilder getPartialJobDtoBuilder() {
    return JobDto.builder()
        .jobId("my_job_id")
        .creationTimeMs(1651016746000L)
        .startTimeMs(1651016750000L)
        .finishTimeMs(1651017746000L)
        .lastUpdateTimeMs(1651017746000L)
        .heartbeatTimeMs(1651017746000L)
        .executionId("1")
        .state(JobState.SUCCEEDED);
  }

  static JobDto getFullJobDto() {
    return getPartialJobDtoBuilder()
        .jobName("my_job")
        .clusterId("my_cluster")
        .jobConf(JobConf.builder().jobType(JobConf.JobType.RETENTION).build())
        .executionId("1")
        .build();
  }
}
