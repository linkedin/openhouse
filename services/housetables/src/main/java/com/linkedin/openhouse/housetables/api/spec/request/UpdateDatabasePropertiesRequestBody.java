package com.linkedin.openhouse.housetables.api.spec.request;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class UpdateDatabasePropertiesRequestBody {
  @Schema(description = "Keys to add or replace. Omitted keys retain their values.")
  @NotNull
  @Builder.Default
  private Map<@NotNull @Size(max = 512) String, @NotNull String> updates = Collections.emptyMap();

  @Schema(description = "Keys to remove. A key must occur in only one of updates and removals.")
  @NotNull
  @Builder.Default
  private Set<@NotNull @Size(max = 512) String> removals = Collections.emptySet();
}
