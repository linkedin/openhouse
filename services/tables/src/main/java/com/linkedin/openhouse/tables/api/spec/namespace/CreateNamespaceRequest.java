package com.linkedin.openhouse.tables.api.spec.namespace;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
public class CreateNamespaceRequest {
  @Schema(required = true, description = "Exactly one namespace name.")
  @NotNull
  @Size(min = 1, max = 1)
  @JsonDeserialize(contentUsing = NamespaceStringDeserializer.class)
  private List<@NotNull @Pattern(regexp = "[a-zA-Z0-9_]{1,128}") String> namespace;

  @NotNull
  @Builder.Default
  @JsonDeserialize(contentUsing = NamespaceStringDeserializer.class)
  private Map<@NotNull @Size(max = 512) @Pattern(regexp = "\\P{Cs}*") String, @NotNull String>
      properties = Collections.emptyMap();
}
