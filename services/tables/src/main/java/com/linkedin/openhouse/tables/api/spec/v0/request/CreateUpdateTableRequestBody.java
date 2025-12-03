package com.linkedin.openhouse.tables.api.spec.v0.request;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.*;

import com.google.gson.GsonBuilder;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ClusteringColumn;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.TimePartitionSpec;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.dto.mapper.attribute.PoliciesSpecConverter;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import javax.persistence.Convert;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder(toBuilder = true)
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CreateUpdateTableRequestBody {

  @Schema(
      description = "Unique Resource identifier for a table within a Database",
      example = "my_table")
  @NotEmpty(message = "tableId cannot be empty")
  @Size(max = 128)
  @Pattern(regexp = ALPHA_NUM_UNDERSCORE_REGEX, message = ALPHA_NUM_UNDERSCORE_ERROR_MSG)
  private String tableId;

  @Schema(
      description = "Unique Resource identifier for the Database containing the Table",
      example = "my_database")
  @Size(max = 128)
  @NotEmpty(message = "databaseId cannot be empty")
  @Pattern(regexp = ALPHA_NUM_UNDERSCORE_REGEX, message = ALPHA_NUM_UNDERSCORE_ERROR_MSG)
  private String databaseId;

  @Schema(
      description = "Unique Resource identifier for the Cluster containing the Database",
      example = "my_cluster")
  @NotEmpty(message = "clusterId cannot be empty")
  @Pattern(
      regexp = ALPHA_NUM_UNDERSCORE_REGEX_HYPHEN_ALLOW,
      message = ALPHA_NUM_UNDERSCORE_ERROR_MSG_HYPHEN_ALLOW)
  private String clusterId;

  @Schema(
      description = "Schema of the table. OpenHouse tables use Iceberg schema specification",
      example =
          "{\"type\": \"struct\", "
              + "\"fields\": [{\"id\": 1,\"required\": true,\"name\": \"id\",\"type\": \"string\"}, "
              + "{\"id\": 2,\"required\": true,\"name\": \"name\", \"type\": \"string\"}, "
              + "{\"id\": 3,\"required\": true,\"name\":\"timestampColumn\",\"type\": \"timestamp\"}]}")
  @NotEmpty(message = "schema cannot be empty")
  private String schema;

  @Schema(
      nullable = true,
      description =
          "Map of intermediate schemas to store when updating multiple schemas at once, typically through replication. "
              + "This is used to preserve schema history when multiple schema updates occur in a single commit.",
      example =
          "{\"1\": \"{\\\"type\\\": \\\"struct\\\", \\\"fields\\\": [...]}\", \"2\": \"{...}\"}")
  private Map<String, String> intermediateSchemas;

  @Schema(
      nullable = true,
      description = "Time partitioning of the table",
      example = "\"timePartitioning\":{\"columnName\":\"timestampCol\",\"granularity\":\"HOUR\"}")
  @Valid
  private TimePartitionSpec timePartitioning;

  @Schema(
      nullable = true,
      description = "Clustering columns for the table",
      example =
          "\"clustering\":[{\"columnName\":\"country\"},"
              + "{\"columnName\":\"city\",\"transform\":{\"transformType\":\"TRUNCATE\",\"transformParams\":[\"1000\"]}}]")
  @Valid
  private List<ClusteringColumn> clustering;

  @Schema(description = "Table properties", example = "{\"key\": \"value\"}")
  @NotNull
  private Map<String, String> tableProperties;

  @Schema(nullable = true, description = "Policies of the table")
  @Convert(converter = PoliciesSpecConverter.class)
  @Valid
  private Policies policies;

  @Schema(
      defaultValue = "false",
      description = "Boolean that determines creating a staged table",
      example = "false")
  @Builder.Default
  private boolean stageCreate = false;

  @Schema(description = "The version of table that the current update is based upon")
  @NotEmpty(message = "baseTableVersion cannot be empty")
  private String baseTableVersion;

  @Schema(description = "The type of a table")
  @Valid
  @Builder.Default
  private TableType tableType = TableType.PRIMARY_TABLE;

  @Schema(
      nullable = true,
      description = "The sort order of a table",
      example =
          "\"sortOrder\":{\"order-id\":1,\"fields\":[{\"transform\":\"identity\",\"source-id\":1,\"direction\":\"asc\",\"null-order\":\"nulls-first\"}]}")
  @Valid
  private String sortOrder;

  public String toJson() {
    return new GsonBuilder().serializeNulls().create().toJson(this);
  }
}
