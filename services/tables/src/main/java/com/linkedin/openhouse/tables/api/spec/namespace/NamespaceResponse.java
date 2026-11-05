package com.linkedin.openhouse.tables.api.spec.namespace;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class NamespaceResponse {
  @Schema(required = true)
  private final List<String> namespace;

  @Schema(nullable = true)
  private final Map<String, String> properties;
}
