package com.linkedin.openhouse.housetables.api.spec.response;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class NamespaceMetadataResponse {
  @Schema(description = "Immutable server-generated UUID.", required = true)
  String namespaceUuid;

  @Schema(description = "Canonical lowercase namespace name.", required = true)
  String namespaceName;

  @Schema(description = "Namespace properties with case-sensitive keys.", required = true)
  Map<String, String> properties;
}
