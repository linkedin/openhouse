package com.linkedin.openhouse.housetables.api.spec.request;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Map;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class UpdateNamespaceRequestBody {
  @Schema(description = "Immutable namespace UUID.", required = true)
  @NotNull
  private String namespaceUuid;

  @Schema(description = "Namespace name with its original insertion spelling.", required = true)
  @NotNull
  private String namespaceName;

  @Schema(description = "Complete replacement properties with non-null values.", required = true)
  @NotNull
  private Map<String, @NotNull String> properties;

  @Schema(description = "Opaque revision returned by the preceding read.", required = true)
  @NotNull
  private String revision;
}
