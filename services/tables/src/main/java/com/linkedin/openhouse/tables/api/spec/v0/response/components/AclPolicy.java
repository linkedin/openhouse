package com.linkedin.openhouse.tables.api.spec.v0.response.components;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class AclPolicy {
  @Schema(description = "Principal with the role on the table/database")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String principal;

  @Schema(description = "Role associated with the principal")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String role;

  @Schema(description = "Optional properties to accept key-value pair")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  @Nullable
  private Map<String, String> properties;
}
