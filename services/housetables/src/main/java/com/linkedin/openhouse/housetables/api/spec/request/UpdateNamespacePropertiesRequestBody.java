package com.linkedin.openhouse.housetables.api.spec.request;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class UpdateNamespacePropertiesRequestBody {
  @Schema(description = "Keys to add or replace. Omitted keys retain their values.")
  @Builder.Default
  private Map<String, String> updates = Collections.emptyMap();

  @Schema(description = "Keys to remove.")
  @Builder.Default
  private Set<String> removals = Collections.emptySet();
}
