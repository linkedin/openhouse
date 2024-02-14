package com.linkedin.openhouse.jobs.mock;

import com.linkedin.openhouse.housetables.client.model.Job;
import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.api.spec.response.JobResponseBody;
import com.linkedin.openhouse.jobs.dto.mapper.JobsMapper;
import com.linkedin.openhouse.jobs.dto.mapper.JobsMapperImpl;
import com.linkedin.openhouse.jobs.model.JobConfConverter;
import com.linkedin.openhouse.jobs.model.JobDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JobsMapperTest {
  protected JobsMapper mapper = new JobsMapperImpl();

  private static final JobDto JOB_DTO = JobModelConstants.getFullJobDto();
  private static final JobResponseBody GET_JOB_RESPONSE_BODY = toGetResponseBody(JOB_DTO);
  private static final CreateJobRequestBody CREATE_JOB_REQUEST_BODY =
      toCreateJobRequestBody(JOB_DTO);
  private static final Job JOB = toJob(JOB_DTO);

  @Test
  public void testToGetJobResponseBody() {
    Assertions.assertEquals(GET_JOB_RESPONSE_BODY, mapper.toGetJobResponseBody(JOB_DTO));
  }

  @Test
  public void testToJobDto() {
    Assertions.assertEquals(
        JOB_DTO,
        mapper.toJobDto(
            JobModelConstants.getPartialJobDtoBuilder().build(), CREATE_JOB_REQUEST_BODY));
  }

  @Test
  public void testToJob() {
    Assertions.assertEquals(JOB, mapper.toJob(JOB_DTO));
  }

  @Test
  public void testToJobDtoMapper() {
    Assertions.assertEquals(JOB_DTO, mapper.toJobDto(JOB));
  }

  private static JobResponseBody toGetResponseBody(JobDto job) {
    return JobResponseBody.builder()
        .jobId(job.getJobId())
        .jobName(job.getJobName())
        .clusterId(job.getClusterId())
        .creationTimeMs(job.getCreationTimeMs())
        .startTimeMs(job.getStartTimeMs())
        .finishTimeMs(job.getFinishTimeMs())
        .lastUpdateTimeMs(job.getLastUpdateTimeMs())
        .state(job.getState())
        .jobConf(job.getJobConf())
        .executionId(job.getExecutionId())
        // internal heartbeatTimeMs and sessionId are omitted
        .build();
  }

  private static CreateJobRequestBody toCreateJobRequestBody(JobDto job) {
    return CreateJobRequestBody.builder()
        .jobName(job.getJobName())
        .clusterId(job.getClusterId())
        .jobConf(job.getJobConf())
        .build();
  }

  private static Job toJob(JobDto jobDto) {
    return new Job()
        .jobId(jobDto.getJobId())
        .jobName(jobDto.getJobName())
        .jobConf(new JobConfConverter().convertToDatabaseColumn(jobDto.getJobConf()))
        .state(jobDto.getState().name())
        .clusterId(jobDto.getClusterId())
        .creationTimeMs(jobDto.getCreationTimeMs())
        .startTimeMs(jobDto.getStartTimeMs())
        .finishTimeMs(jobDto.getFinishTimeMs())
        .lastUpdateTimeMs(jobDto.getLastUpdateTimeMs())
        .heartbeatTimeMs(jobDto.getHeartbeatTimeMs())
        .executionId(jobDto.getExecutionId());
  }
}
