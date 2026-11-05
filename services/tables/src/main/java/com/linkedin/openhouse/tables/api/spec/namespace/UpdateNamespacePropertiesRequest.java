package com.linkedin.openhouse.tables.api.spec.namespace;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.validation.constraints.NotNull;
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
  @NotNull
  @Builder.Default
  @JsonDeserialize(contentUsing = NamespaceStringDeserializer.class)
  private Map<@NotNull @Size(max = 512) String, @NotNull String> updates = Collections.emptyMap();

  @NotNull
  @Builder.Default
  @JsonDeserialize(contentUsing = NamespaceStringDeserializer.class)
  private Set<@NotNull @Size(max = 512) String> removals = Collections.emptySet();
}
