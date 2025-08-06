package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class GetSoftDeletedTableResponseBody {

  @Schema(
      description = "Unique Resource identifier for a table within a Database",
      example = "my_table")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String tableId;

  @Schema(
      description = "Unique Resource identifier for the Database containing the Table",
      example = "my_database")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String databaseId;

  @Schema(
      description = "Unix Timestamp in milliseconds when the table was dropped",
      example = "1753915710")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private long deletedAtMs;

  @Schema(
      description = "Location of Table in File System / Blob Store",
      example =
          "<fs>://<hostname>/<openhouse_namespace>/<database_name>/<tableUUID>/metadata/<uuid>.metadata.json")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String tableLocation;

  @Schema(
      description =
          "Unix Timestamp in milliseconds after which the table is marked for permanent deletion",
      example = "1753915710")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private long purgeAfterMs;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
