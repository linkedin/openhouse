package com.linkedin.openhouse.housetables.api.spec.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Size;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class ReplicationConfiguration {
  @Schema(description = "Destination cluster identifier", example = "CLUSTERA")
  @JsonProperty("destinationClusterId")
  @NotEmpty
  @Size(max = 128)
  private String destinationClusterId;

  @Schema(description = "Destination database identifier", example = "my_database")
  @JsonProperty("destinationDatabaseId")
  @NotEmpty
  @Size(max = 128)
  private String destinationDatabaseId;

  @Schema(description = "Destination table identifier", example = "my_table")
  @JsonProperty("destinationTableId")
  @NotEmpty
  @Size(max = 128)
  private String destinationTableId;

  @Schema(description = "Canonical replication interval", example = "12H")
  @JsonProperty("replicationInterval")
  @NotEmpty
  @Size(max = 128)
  private String replicationInterval;
}
