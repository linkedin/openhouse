package com.linkedin.openhouse.tables.api.spec.v0.request.components;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder(toBuilder = true)
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Transform {
  @Schema(description = "Type of the transform", example = "TRUNCATE")
  @NotNull(message = "transformType cannot be null")
  TransformType transformType;

  @Schema(
      nullable = true,
      description = "Parameters of the transform. This field can be null",
      example = "[\"1000\"]")
  List<String> transformParams;

  public enum TransformType {
    TRUNCATE,
    BUCKET
  }
}
