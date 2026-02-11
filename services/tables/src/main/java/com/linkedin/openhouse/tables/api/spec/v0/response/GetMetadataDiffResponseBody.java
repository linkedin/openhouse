package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

/**
 * Response body for metadata diff endpoint containing current and previous metadata.json content
 */
@Builder(toBuilder = true)
@Value
public class GetMetadataDiffResponseBody {

  @Schema(description = "Table ID", example = "my_table")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String tableId;

  @Schema(description = "Database ID", example = "my_database")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String databaseId;

  @Schema(
      description = "Current metadata.json content",
      example = "{\"format-version\":2,\"table-uuid\":\"...\",\"location\":\"...\"}")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String currentMetadata;

  @Schema(description = "Current metadata timestamp in milliseconds")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Long currentTimestamp;

  @Schema(description = "Current metadata file location")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String currentMetadataLocation;

  @Schema(
      description = "Previous metadata.json content (null if first entry in metadata-log)",
      example = "{\"format-version\":2,\"table-uuid\":\"...\",\"location\":\"...\"}")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String previousMetadata;

  @Schema(description = "Previous metadata timestamp in milliseconds (null if first entry)")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Long previousTimestamp;

  @Schema(description = "Previous metadata file location (null if first entry)")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String previousMetadataLocation;

  @Schema(
      description = "Indicates if this is the first entry in metadata-log (no previous metadata)")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Boolean isFirstCommit;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
