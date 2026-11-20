package com.linkedin.openhouse.tables.api.spec.namespace;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpdateNamespacePropertiesRequest {
  @NotNull @Builder.Default
  private Map<@NotNull @Size(max = 512) @Pattern(regexp = "\\P{Cs}*") String, @NotNull String>
      updates = Collections.emptyMap();

  @NotNull @Builder.Default
  private Set<@NotNull @Size(max = 512) @Pattern(regexp = "\\P{Cs}*") String> removals =
      Collections.emptySet();
}
