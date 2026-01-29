package com.linkedin.openhouse.jobs.api.handler.impl;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.jobs.api.handler.JobsApiHandler;
import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.api.spec.response.JobResponseBody;
import com.linkedin.openhouse.jobs.api.spec.response.JobSearchResponseBody;
import com.linkedin.openhouse.jobs.api.validator.JobsApiValidator;
import com.linkedin.openhouse.jobs.dto.mapper.JobsMapper;
import com.linkedin.openhouse.jobs.model.JobDto;
import com.linkedin.openhouse.jobs.services.JobsService;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class OpenHouseJobsApiHandler implements JobsApiHandler {
  @Autowired private JobsApiValidator apiValidator;
  @Autowired private JobsService service;
  @Autowired private JobsMapper mapper;

  @Override
  public ApiResponse<JobResponseBody> get(String jobId) {
    return ApiResponse.<JobResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(mapper.toGetJobResponseBody(service.get(jobId)))
        .build();
  }

  @Override
  public ApiResponse<JobResponseBody> create(CreateJobRequestBody createJobRequestBody) {
    apiValidator.validateCreateJob(createJobRequestBody);
    JobDto jobDto = service.create(createJobRequestBody);
    return ApiResponse.<JobResponseBody>builder()
        .httpStatus(HttpStatus.CREATED)
        .responseBody(mapper.toGetJobResponseBody(jobDto))
        .build();
  }

  @Override
  public ApiResponse<Void> cancel(String jobId) {
    service.cancel(jobId);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<JobSearchResponseBody> search(String jobNamePrefix, int limit) {
    List<JobDto> jobs = service.search(jobNamePrefix, limit);
    List<JobResponseBody> results =
        jobs.stream().map(mapper::toGetJobResponseBody).collect(Collectors.toList());
    return ApiResponse.<JobSearchResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(JobSearchResponseBody.builder().results(results).build())
        .build();
  }
}
