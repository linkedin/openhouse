package com.linkedin.openhouse.housetables.api.spec.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class ReplicationConfigurationSet {
  @Schema(description = "Source database identifier", example = "my_database")
  @JsonProperty("sourceDatabaseId")
  @NotEmpty
  @Size(max = 128)
  private String sourceDatabaseId;

  @Schema(description = "Source table identifier", example = "my_table")
  @JsonProperty("sourceTableId")
  @NotEmpty
  @Size(max = 128)
  private String sourceTableId;

  @Schema(
      description =
          "True when a replication policy is present. False represents an explicit policy clear; "
              + "an empty configuration list represents a present policy with no destinations.")
  @JsonProperty("configured")
  @NotNull
  private Boolean configured;

  @Schema(description = "Per-destination replication configuration")
  @JsonProperty("configurations")
  @NotNull
  @Valid
  private List<ReplicationConfiguration> configurations;
}
