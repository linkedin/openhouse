package com.linkedin.openhouse.jobs.api.spec.response;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.*;

import com.google.gson.Gson;
import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.jobs.model.JobConf;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Pattern;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class JobResponseBody {
  @Schema(
      description = "Unique auto-generated identifier for job prefixed with jobName",
      example = "my_job_8347adee-65b7-4e05-86fc-196af04f4e68")
  @NotEmpty(message = "jobId cannot be empty")
  @Pattern(
      regexp = ALPHA_NUM_UNDERSCORE_REGEX_HYPHEN_ALLOW,
      message = ALPHA_NUM_UNDERSCORE_ERROR_MSG_HYPHEN_ALLOW)
  private String jobId;

  @Schema(description = "Name of a job, doesn't need to be unique", example = "my_job")
  @NotEmpty(message = "jobName cannot be empty")
  @Pattern(
      regexp = ALPHA_NUM_UNDERSCORE_REGEX_HYPHEN_ALLOW,
      message = ALPHA_NUM_UNDERSCORE_ERROR_MSG_HYPHEN_ALLOW)
  private String jobName;

  @Schema(description = "Unique identifier for the cluster", example = "my_cluster")
  @NotEmpty(message = "clusterId cannot be empty")
  @Pattern(
      regexp = ALPHA_NUM_UNDERSCORE_REGEX_HYPHEN_ALLOW,
      message = ALPHA_NUM_UNDERSCORE_ERROR_MSG_HYPHEN_ALLOW)
  private String clusterId;

  @Schema(
      description =
          "Current job state, possible states: QUEUED, ACTIVE, CANCELLED, FAILED, SUCCEEDED",
      example = "SUCCEEDED")
  @NotEmpty(message = "state cannot be empty")
  private JobState state;

  @Schema(description = "Job creation time in unix epoch milliseconds", example = "1651002318265")
  @NotEmpty(message = "creationTimeMs cannot be empty")
  private long creationTimeMs;

  @Schema(description = "Job start time in unix epoch milliseconds", example = "1651002318265")
  private long startTimeMs;

  @Schema(description = "Job finish time in unix epoch milliseconds", example = "1651002318265")
  private long finishTimeMs;

  @Schema(
      description = "Job contents last update time in unix epoch milliseconds",
      example = "1651002318265")
  private long lastUpdateTimeMs;

  @Schema(description = "Job config", example = "{'jobType': 'RETENTION', 'table': 'db.tb'}")
  private JobConf jobConf;

  @Schema(description = "Execution ID generated from engine where job is submitted")
  @Nullable
  private String executionId;

  @Schema(description = "Engine type that job submitted to", example = "LIVY")
  @Nullable
  private String engineType;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
