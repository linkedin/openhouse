package com.linkedin.openhouse.housetables.api.spec.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import lombok.Builder;
import lombok.Value;

/**
 * The row type for the House table storing jobs. {@link JsonProperty} annotation is needed when a
 * field is declared as private to instruct jackson library initialize and deserialize the model
 * object without no-arg constructor provided.
 */
@Builder(toBuilder = true)
@Value
public class Job {
  @Schema(
      description = "Unique Resource identifier for a job within a Database.",
      example = "24efc962-9962-4522-b0b6-29490d7d8a0e")
  @JsonProperty(value = "jobId")
  @NotEmpty(message = "jobId cannot be empty")
  private String jobId;

  @Schema(description = "State for the job", example = "QUEUED")
  @JsonProperty(value = "state")
  private String state;

  @Schema(
      description =
          "Version of the job entry in HTS. HTS internally generates the next version after a successful PUT."
              + "The value would be a string representing a random integer.",
      example = "539482")
  @JsonProperty(value = "version")
  private String version;

  @Schema(description = "Name of a job, doesn't need to be unique", example = "my_job")
  @JsonProperty(value = "jobName")
  @NotEmpty(message = "jobName cannot be empty")
  private String jobName;

  @Schema(description = "Unique identifier for the cluster", example = "my_cluster")
  @JsonProperty(value = "clusterId")
  @NotEmpty(message = "clusterId cannot be empty")
  private String clusterId;

  @Schema(description = "Job creation time in unix epoch milliseconds", example = "1651002318265")
  @JsonProperty(value = "creationTimeMs")
  private Long creationTimeMs;

  @Schema(description = "Job start time in unix epoch milliseconds", example = "1651002318265")
  @JsonProperty(value = "startTimeMs")
  private Long startTimeMs;

  @Schema(description = "Job finish time in unix epoch milliseconds", example = "1651002318265")
  @JsonProperty(value = "finishTimeMs")
  private Long finishTimeMs;

  @Schema(
      description = "Job contents last update time in unix epoch milliseconds",
      example = "1651002318265")
  @JsonProperty(value = "lastUpdateTimeMs")
  private Long lastUpdateTimeMs;

  @Schema(description = "Job config", example = "{'jobType': 'RETENTION', 'table': 'db.tb'}")
  @JsonProperty(value = "jobConf")
  private String jobConf;

  @Schema(
      description = "Running job heartbeat timestamp in milliseconds",
      example = "1651002318265")
  @JsonProperty(value = "heartbeatTimeMs")
  private Long heartbeatTimeMs;

  @Schema(
      description = "Launched job execution id specific to engine",
      example = "application_1642969576960_13278206")
  @JsonProperty(value = "executionId")
  private String executionId;
}
