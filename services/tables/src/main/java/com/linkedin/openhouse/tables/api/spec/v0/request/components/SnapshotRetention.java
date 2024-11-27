package com.linkedin.openhouse.tables.api.spec.v0.request.components;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.Valid;
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
public class SnapshotRetention {
  @Schema(
      description =
          "time period in count <granularity> for which the retention on table will be applied",
      example = "3,4,5")
  @Positive(message = "Incorrect count specified. retention.timeCount has to be a positive integer")
  @Valid
  int timeCount;

  @Schema(
      description = "time period granularity for which the retention on table will be applied",
      example = "hour, day, month, year")
  @Valid
  TimePartitionSpec.Granularity granularity;

  @Schema(
      description = "minimum number of snapshots to keep within history for the table",
      example = "3,4,5")
  @Positive(
      message = "Incorrect count specified. retention.versionCount has to be a positive integer")
  int versionCount;

  @Valid
  @Schema(
      description =
          "Object that is required when defining both timeCount and versionCount. It specifies the logical operator to be used between the two retention policies",
      example = "AND, OR")
  LogicalOperator logicalOperator;
}
