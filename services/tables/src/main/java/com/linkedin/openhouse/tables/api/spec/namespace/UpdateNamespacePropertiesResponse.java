package com.linkedin.openhouse.tables.api.spec.namespace;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class UpdateNamespacePropertiesResponse {
  @Schema(required = true)
  private final List<String> updated;

  @Schema(required = true)
  private final List<String> removed;

  @Schema(nullable = true)
  private final List<String> missing;
}
