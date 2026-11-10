package com.linkedin.openhouse.housetables.api.spec.model;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DatabaseMetadata {
  @Schema(description = "Immutable server-generated UUID.", required = true)
  String databaseUuid;

  @Schema(description = "Canonical lowercase database name.", required = true)
  String databaseName;

  @Schema(description = "Database properties with case-sensitive keys.", required = true)
  Map<String, String> properties;
}
