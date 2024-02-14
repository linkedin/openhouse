package com.linkedin.openhouse.housetables.api.spec.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import lombok.Builder;
import lombok.Value;

/** The key type for the House table storing jobs. */
@Builder
@Value
public class JobKey {

  @Schema(
      description = "Unique Resource identifier for a job within a Database.",
      example = "24efc962-9962-4522-b0b6-29490d7d8a0e")
  @JsonProperty(value = "jobId")
  @NotEmpty(message = "jobId cannot be empty")
  private String jobId;
}
