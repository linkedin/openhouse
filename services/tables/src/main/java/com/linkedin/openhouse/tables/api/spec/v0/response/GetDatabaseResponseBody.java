package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class GetDatabaseResponseBody {
  @Schema(description = "Unique Resource identifier for the Database", example = "my_database")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String databaseId;

  @Schema(
      description = "Unique Resource identifier for the Cluster containing the Database",
      example = "my_cluster")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String clusterId;
}
