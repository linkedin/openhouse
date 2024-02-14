package com.linkedin.openhouse.jobs.api.spec.request;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.*;

import com.google.gson.Gson;
import com.linkedin.openhouse.jobs.model.JobConf;
import io.swagger.v3.oas.annotations.media.Schema;
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
public class CreateJobRequestBody {
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

  @Schema(description = "Job config", example = "{'jobType': 'RETENTION', 'table': 'db.tb'}")
  private JobConf jobConf;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
