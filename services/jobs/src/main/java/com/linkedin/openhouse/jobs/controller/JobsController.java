package com.linkedin.openhouse.jobs.controller;

import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.jobs.api.handler.JobsApiHandler;
import com.linkedin.openhouse.jobs.api.spec.request.CreateJobRequestBody;
import com.linkedin.openhouse.jobs.api.spec.response.JobResponseBody;
import io.micrometer.core.annotation.Timed;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** Class defining all the REST Endpoints for /jobs endpoint. */
@RestController
public class JobsController {
  @Autowired private JobsApiHandler jobsApiHandler;

  @Operation(
      summary = "Get Job",
      description = "Returns a Job resource identified by jobId.",
      tags = {"Job"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Job GET: OK"),
        @ApiResponse(responseCode = "404", description = "Job GET: NOT_FOUND")
      })
  @GetMapping(
      value = "/jobs/{jobId}",
      produces = {"application/json"})
  @Timed(
      value = MetricsConstant.REQUEST,
      extraTags = {"service", MetricsConstant.JOBS_SERVICE, "action", MetricsConstant.GET})
  public ResponseEntity<JobResponseBody> getJob(
      @Parameter(description = "Job ID", required = true) @PathVariable String jobId) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<JobResponseBody> apiResponse =
        jobsApiHandler.get(jobId);
    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Submit a Job",
      description = "Submits a Job and returns a Job resource.",
      tags = {"Job"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "201", description = "Job POST: CREATED"),
        @ApiResponse(responseCode = "400", description = "Job POST: BAD_REQUEST")
      })
  @PostMapping(
      value = "/jobs",
      produces = {"application/json"},
      consumes = {"application/json"})
  @Timed(
      value = MetricsConstant.REQUEST,
      extraTags = {"service", MetricsConstant.JOBS_SERVICE, "action", MetricsConstant.CREATE})
  public ResponseEntity<JobResponseBody> createJob(
      @Parameter(
              description = "Request containing details of the Job to be created",
              required = true,
              schema = @Schema(implementation = CreateJobRequestBody.class))
          @RequestBody
          CreateJobRequestBody createJobRequestBody) {

    com.linkedin.openhouse.common.api.spec.ApiResponse<JobResponseBody> apiResponse =
        jobsApiHandler.create(createJobRequestBody);

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }

  @Operation(
      summary = "Cancel Job",
      description = "Cancels the job given jobId",
      tags = {"Job"})
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Job PUT: UPDATED"),
        @ApiResponse(responseCode = "404", description = "Job PUT: NOT_FOUND"),
        @ApiResponse(responseCode = "409", description = "Job PUT: CONFLICT")
      })
  @PutMapping(value = "/jobs/{jobId}/cancel")
  @Timed(
      value = MetricsConstant.REQUEST,
      extraTags = {"service", MetricsConstant.JOBS_SERVICE, "action", MetricsConstant.CANCEL})
  public ResponseEntity<Void> cancelJob(
      @Parameter(description = "Job ID", required = true) @PathVariable String jobId) {
    com.linkedin.openhouse.common.api.spec.ApiResponse<Void> apiResponse =
        jobsApiHandler.cancel(jobId);

    return new ResponseEntity<>(
        apiResponse.getResponseBody(), apiResponse.getHttpHeaders(), apiResponse.getHttpStatus());
  }
}
