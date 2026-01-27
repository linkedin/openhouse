package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.Builder;
import lombok.Value;

/** Response body for internal table metadata endpoint containing rich Iceberg metadata */
@Builder(toBuilder = true)
@Value
public class GetIcebergMetadataResponseBody {

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

  @Schema(
      description = "List of previous metadata.json files with their versions",
      example = "[{\"version\":1,\"file\":\"v1.metadata.json\",\"timestamp\":1651002318265}]")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private List<MetadataVersion> metadataHistory;

  @Schema(description = "Current metadata file location")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String metadataLocation;

  @Schema(description = "Snapshots information")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String snapshots;

  @Schema(description = "Partitions information")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String partitions;

  @Schema(description = "Current snapshot ID")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Long currentSnapshotId;

  public String toJson() {
    return new Gson().toJson(this);
  }

  /** Represents a metadata version entry */
  @Builder
  @Value
  public static class MetadataVersion {
    @Schema(description = "Metadata version number")
    private Integer version;

    @Schema(description = "Metadata file name", example = "v1.metadata.json")
    private String file;

    @Schema(description = "Timestamp when this version was created")
    private Long timestamp;

    @Schema(description = "Full path to metadata file")
    private String location;
  }
}
