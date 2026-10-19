package com.linkedin.openhouse.jobs.dto.mapper;

import com.linkedin.openhouse.housetables.client.model.Job;
import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.api.spec.response.JobResponseBody;
import com.linkedin.openhouse.jobs.model.JobConfConverter;
import com.linkedin.openhouse.jobs.model.JobDto;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;

/** Mapper class to transform between DTO and Data Model objects. */
@Mapper(
    componentModel = "spring",
    imports = {JobConfConverter.class})
public interface JobsMapper {

  /**
   * Update elements in {@link JobDto} based on {@link
   * com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody}
   *
   * @param jobDto Destination {@link JobDto} that will be updated.
   * @param requestBody Source {@link
   *     com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody}
   * @return A new immutable {@link JobDto} with updated elements.
   */
  @Mappings({
    @Mapping(source = "requestBody.jobName", target = "jobName"),
    @Mapping(source = "requestBody.clusterId", target = "clusterId"),
    @Mapping(source = "requestBody.jobConf", target = "jobConf")
  })
  JobDto toJobDto(JobDto jobDto, CreateJobRequestBody requestBody);

  /**
   * From a source {@link JobDto}, prepare a {@link JobResponseBody}
   *
   * @param jobDto Source {@link JobDto} to transform.
   * @return Destination {@link JobResponseBody} to be forwarded to the client.
   */
  JobResponseBody toGetJobResponseBody(JobDto jobDto);

  @Mapping(
      target = "jobConf",
      expression = "java(new JobConfConverter().convertToDatabaseColumn(jobDto.getJobConf()))")
  Job toJob(JobDto jobDto);

  @Mapping(
      target = "jobConf",
      expression = "java(new JobConfConverter().convertToEntityAttribute(job.getJobConf()))")
  JobDto toJobDto(Job job);
}
