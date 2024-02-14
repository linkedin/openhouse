package com.linkedin.openhouse.tables.api.spec.v0.request.components;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ClusteringColumn {
  @Schema(
      description =
          "Name of the clustering column in provided schema. The column should be of the type 'String'."
              + "Nested columns can also be provided with a dot-separated name (for example: 'eventHeader.countryCode')."
              + "Column name is case-sensitive.",
      example = "clusteringColumn")
  @NotEmpty(message = "columnName cannot be empty")
  @NotNull(message = "columnName cannot be null")
  String columnName;
}
