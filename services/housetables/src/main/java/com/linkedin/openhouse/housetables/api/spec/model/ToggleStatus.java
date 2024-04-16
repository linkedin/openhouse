package com.linkedin.openhouse.housetables.api.spec.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.openhouse.common.api.spec.ToggleStatusEnum;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import lombok.Builder;
import lombok.Value;

/** This layer on top of {@link ToggleStatusEnum} is ensuring API extensibility. */
@Builder(toBuilder = true)
@Value
public class ToggleStatus {
  @Schema(
      description = "Status of an entity with respect to whether a feature has been toggled on",
      example = "Active")
  @JsonProperty(value = "status")
  @NotEmpty(message = "Toggle status cannot be empty")
  ToggleStatusEnum status;
}
