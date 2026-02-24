package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

/** Response body for table data endpoint containing the first N rows of an Iceberg table */
@Builder(toBuilder = true)
@Value
public class GetTableDataResponseBody {

  @Schema(description = "Table ID", example = "my_table")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String tableId;

  @Schema(description = "Database ID", example = "my_database")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String databaseId;

  @Schema(
      description = "Table schema as JSON string",
      example = "{\"type\":\"struct\",\"fields\":[...]}")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String schema;

  @Schema(
      description = "List of rows, each row is a map of column name to value",
      example = "[{\"col1\": \"value1\", \"col2\": 123}]")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private List<Map<String, Object>> rows;

  @Schema(description = "Number of rows actually fetched", example = "100")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Integer totalRowsFetched;

  @Schema(description = "Whether there are more rows available beyond the limit", example = "true")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private Boolean hasMore;
}
