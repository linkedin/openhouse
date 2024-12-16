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
      description = "Time period in count <granularity> to keep the snapshot history on the table",
      example = "3,4,5")
  @PositiveOrZero(
      message = "Incorrect count specified. retention.maxAge has to be a positive integer")
  @Valid
  int maxAge;

  @Schema(description = "time period granularity for the snapshot history", example = "hour, day")
  @Valid
  TimePartitionSpec.Granularity granularity;

  @Schema(
      description =
          "Number of snapshots to keep within history for the table after snapshot expiration",
      example = "3,4,5")
  @PositiveOrZero(
      message = "Incorrect count specified. history.versions has to be a positive integer")
  int versions;
}
