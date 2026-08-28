package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

/**
 * Read contract for a view. Pointer-only today: this response omits the definition fields. The SQL,
 * schema, representations, version history, UUID, properties and resolution context live in the
 * view metadata file and are not returned by the item or list response in this milestone.
 */
@Builder(toBuilder = true)
@Value
public class GetViewResponseBody {

  @Schema(
      description = "Unique Resource identifier for a view within a Database",
      example = "my_view")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String viewId;

  @Schema(
      description = "Unique Resource identifier for the Database containing the View",
      example = "my_database")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String databaseId;

  @Schema(
      description = "Unique Resource identifier for the Cluster containing the Database",
      example = "my_cluster")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String clusterId;

  @Schema(
      description = "Fully Qualified Resource URI for the view",
      example = "my_cluster.my_database.my_view")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String viewUri;

  @Schema(
      description = "Location of the view metadata in File System / Blob Store",
      example =
          "<fs>://<hostname>/<openhouse_namespace>/<database_name>/<viewUUID>/metadata/<uuid>.metadata.json")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String metadataLocation;

  @Schema(description = "Current Version of the View.", example = "")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String viewVersion;

  @Schema(
      description = "View creation epoch time measured in UTC in milliseconds of a view.",
      example = "1651002318265")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private long creationTime;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
