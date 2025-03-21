package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.common.TableType;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class GetTableResponseBody {

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
      description = "Unique Resource identifier for the Cluster containing the Database",
      example = "my_cluster")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String clusterId;

  @Schema(
      description = "Fully Qualified Resource URI for the table",
      example = "my_cluster.my_database.my_table")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String tableUri;

  @Schema(description = "Table UUID", example = "73ea0d21-3c89-4987-a6cf-26e4f86bdcee")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String tableUUID;

  @Schema(
      description = "Location of Table in File System / Blob Store",
      example =
          "<fs>://<hostname>/<openhouse_namespace>/<database_name>/<tableUUID>/metadata/<uuid>.metadata.json")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String tableLocation;

  @Schema(description = "Current Version of the Table.", example = "")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String tableVersion;

  @Schema(description = "Authenticated user principal that created the Table.", example = "bob")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String tableCreator;

  @Schema(
      description = "Schema of the Table in Iceberg",
      example =
          "{\"type\": \"struct\", "
              + "\"fields\": [{\"id\": 1,\"required\": true,\"name\": \"id\",\"type\": \"string\"}, "
              + "{\"id\": 2,\"required\": true,\"name\": \"name\", \"type\": \"string\"}]}")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String schema;

  @Schema(
      description = "Last modification epoch time in UTC measured in milliseconds of a table.",
      example = "1651002318265")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private long lastModifiedTime;

  @Schema(
      description = "Table creation epoch time measured in UTC in milliseconds of a table.",
      example = "1651002318265")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private long creationTime;

  @Schema(description = "A map of table properties", example = "{\"key\": \"value\"}")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Map<String, String> tableProperties;

  @Schema(nullable = true, description = "Time partitioning of the table")
  private TimePartitionSpec timePartitioning;

  @Schema(nullable = true, description = "Clustering columns for the table")
  private List<ClusteringColumn> clustering;

  @Schema(nullable = true, description = "Table policies")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Policies policies;

  @Schema(description = "The type of a table", example = "PRIMARY_TABLE")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private TableType tableType;;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
