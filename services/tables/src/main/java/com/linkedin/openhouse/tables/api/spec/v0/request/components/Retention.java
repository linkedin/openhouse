package com.linkedin.openhouse.tables.api.spec.v0.request.components;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
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
public class Retention {
  @Schema(
      description =
          "time period in count <granularity> for which the retention on table will be applied",
      example = "3,4,5")
  @Positive(message = "Incorrect count specified. retention.count has to be a positive integer")
  @NotNull(message = "Incorrect count specified. retention.count cannot be null")
  @Valid
  int count;

  @Schema(
      description = "time period granularity for which the retention on table will be applied",
      example = "hour, day, month, year")
  @NotNull(message = "Incorrect granularity specified. retention.granularity cannot be null")
  @Valid
  TimePartitionSpec.Granularity granularity;

  @Valid
  @Schema(
      description =
          "Optional object to specify retention column in case where timestamp is represented as a string",
      example = "{columnName:datepartition, pattern: yyyy-MM-dd-HH}")
  RetentionColumnPattern columnPattern;
}
