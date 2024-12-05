package com.linkedin.openhouse.tables.api.spec.v0.request.components;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.Valid;
import javax.validation.constraints.PositiveOrZero;
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
public class History {
  @Schema(
      description =
          "time period in count <granularity> for which the retention on table will be applied",
      example = "3,4,5")
  @PositiveOrZero(
      message = "Incorrect count specified. retention.maxAge has to be a positive integer")
  @Valid
  int maxAge;

  @Schema(
      description = "time period granularity for which the retention on table will be applied",
      example = "hour, day, month, year")
  @Valid
  TimePartitionSpec.Granularity granularity;

  @Schema(
      description = "minimum number of snapshots to keep within history for the table",
      example = "3,4,5")
  @PositiveOrZero(
      message = "Incorrect count specified. retention.minVersions has to be a positive integer")
  int minVersions;
}
