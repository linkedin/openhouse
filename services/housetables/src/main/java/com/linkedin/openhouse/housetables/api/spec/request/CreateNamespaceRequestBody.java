package com.linkedin.openhouse.housetables.api.spec.request;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collections;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CreateNamespaceRequestBody {
  @Schema(required = true, description = "Logical namespace name.")
  private String namespaceName;

  @Schema(description = "Initial properties. Omitted properties default to an empty map.")
  @Builder.Default
  private Map<String, String> properties = Collections.emptyMap();
}
