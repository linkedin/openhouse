package com.linkedin.openhouse.housetables.controller;

import com.linkedin.openhouse.housetables.api.handler.JobTableHtsApiHandler;
import com.linkedin.openhouse.housetables.api.spec.model.Job;
import com.linkedin.openhouse.housetables.api.spec.model.JobKey;
import com.linkedin.openhouse.housetables.api.spec.request.CreateUpdateEntityRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.EntityResponseBody;
import com.linkedin.openhouse.housetables.api.spec.response.GetAllEntityResponseBody;
import com.linkedin.openhouse.housetables.dto.mapper.JobMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * JobsTablesController is the controller class for the /hts/jobs endpoint. This class is
 * responsible for handling all the API requests that are specific to Job tables. This API is
 * leveraged by Jobs Service to persist Job metadata. The class uses JobTableHtsApiHandler to
 * delegate the request to the service layer.
 */
@RestController
public class JobTablesController {
  private static final String HTS_JOBS_GENERAL_ENDPOINT = "/hts/jobs";
  private static final String HTS_JOBS_QUERY_ENDPOINT = "/hts/jobs/query";

  @Autowired private JobTableHtsApiHandler jobTableHtsApiHandler;

  @Autowired private JobMapper jobMapper;

  @Operation(
      summary = "Get job details for a given jobId.",
      description = "Returns a Job entity identified by jobId",
      tags = {"Job"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Job GET: OK"),
        @ApiResponse(responseCode = "404", description = "Job GET: NOT_FOUND")
      })
  @GetMapping(
      value = HTS_JOBS_GENERAL_ENDPOINT,
      produces = {"application/json"})
  public ResponseEntity<EntityResponseBody<Job>> getJob(
      @RequestParam(value = "jobId") String jobId) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<EntityResponseBody<Job>> apiResponse =
        jobTableHtsApiHandler.getEntity(JobKey.builder().jobId(jobId).build());

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Search Jobs by filter",
      description =
          "Returns jobs that fulfills the filter predicate. "
              + "For examples, one could provide {status: <QUEUED>} in the map to query all jobs with that status. ",
      tags = {"Job"})
  @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "Job GET: OK")})
  @GetMapping(
      value = HTS_JOBS_QUERY_ENDPOINT,
      produces = {"application/json"})
  public ResponseEntity<GetAllEntityResponseBody<Job>> getAllJobs(
      @RequestParam Map<String, String> parameters) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<GetAllEntityResponseBody<Job>> apiResponse =
        jobTableHtsApiHandler.getEntities(jobMapper.mapToJob(parameters));
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Delete a Job",
      description = "Delete a Job entity identified by jobId",
      tags = {"Job"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "Job DELETE: NO_CONTENT"),
        @ApiResponse(responseCode = "400", description = "Job DELETE: BAD_REQUEST"),
        @ApiResponse(responseCode = "404", description = "Job DELETE: NOT_FOUND")
      })
  @DeleteMapping(value = HTS_JOBS_GENERAL_ENDPOINT)
  public ResponseEntity<Void> deleteJob(@RequestParam(value = "jobId") String jobId) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> apiResponse;
    apiResponse = jobTableHtsApiHandler.deleteEntity(JobKey.builder().jobId(jobId).build(), false);
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Update a Job",
      description =
          "Updates or creates a Job in the House Table "
              + "and returns the job resources. "
              + "If the job does not exist, it will be created. If the job exists, it will be updated.",
      tags = {"Job"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Job PUT: UPDATED"),
        @ApiResponse(responseCode = "201", description = "Job PUT: CREATED"),
        @ApiResponse(responseCode = "400", description = "Job PUT: BAD_REQUEST"),
        @ApiResponse(responseCode = "404", description = "Job PUT: DB_NOT_FOUND"),
        @ApiResponse(responseCode = "409", description = "Job PUT: CONFLICT")
      })
  @PutMapping(
      value = HTS_JOBS_GENERAL_ENDPOINT,
      produces = {"application/json"},
      consumes = {"application/json"})
  public ResponseEntity<EntityResponseBody<Job>> putJob(
      @Parameter(
              description = "Request containing details of the User Table to be created/updated",
              required = true)
          @RequestBody
          CreateUpdateEntityRequestBody<Job> createUpdateJobRequestBody) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<EntityResponseBody<Job>> apiResponse =
        jobTableHtsApiHandler.putEntity(createUpdateJobRequestBody.getEntity());
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }
}
