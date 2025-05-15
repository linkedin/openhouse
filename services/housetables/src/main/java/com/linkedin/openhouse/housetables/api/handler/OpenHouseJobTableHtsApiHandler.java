package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.model.Job;
import com.linkedin.openhouse.housetables.api.spec.model.JobKey;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.api.validator.HouseTablesApiValidator;
import com.linkedin.openhouse.housetables.dto.mapper.JobMapper;
import com.linkedin.openhouse.housetables.dto.model.JobDto;
import com.linkedin.openhouse.housetables.services.JobsService;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class OpenHouseJobTableHtsApiHandler implements JobTableHtsApiHandler {

  @Autowired private HouseTablesApiValidator<JobKey, Job> jobsHTSApiValidator;

  @Autowired private JobsService jobsService;

  @Autowired private JobMapper jobMapper;

  @Override
  public ApiResponse<EntityResponseBody<Job>> getEntity(JobKey key) {
    jobsHTSApiValidator.validateGetEntity(key);
    return ApiResponse.<EntityResponseBody<Job>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            EntityResponseBody.<Job>builder()
                .entity(jobMapper.toJob(jobsService.getJob(key.getJobId())))
                .build())
        .build();
  }

  @Override
  public ApiResponse<GetAllEntityResponseBody<Job>> getEntities(Job entity) {
    jobsHTSApiValidator.validateGetEntities(entity);
    return ApiResponse.<GetAllEntityResponseBody<Job>>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            GetAllEntityResponseBody.<Job>builder()
                .results(
                    jobsService.getAllJobs(entity).stream()
                        .map(jobDto -> jobMapper.toJob(jobDto))
                        .collect(Collectors.toList()))
                .build())
        .build();
  }

  @Override
  public ApiResponse<GetAllEntityResponseBody<Job>> getEntities(
      Job entity, int page, int size, String sortBy) {
    throw new UnsupportedOperationException("Get all job is unsupported");
  }

  @Override
  public ApiResponse<Void> deleteEntity(JobKey key) {
    jobsHTSApiValidator.validateDeleteEntity(key);
    jobsService.deleteJob(key.getJobId());
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<EntityResponseBody<Job>> putEntity(Job entity) {
    jobsHTSApiValidator.validatePutEntity(entity);
    Pair<JobDto, Boolean> putResult = jobsService.putJob(entity);
    HttpStatus statusCode = putResult.getSecond() ? HttpStatus.OK : HttpStatus.CREATED;
    return ApiResponse.<EntityResponseBody<Job>>builder()
        .httpStatus(statusCode)
        .responseBody(
            EntityResponseBody.<Job>builder().entity(jobMapper.toJob(putResult.getFirst())).build())
        .build();
  }
}
