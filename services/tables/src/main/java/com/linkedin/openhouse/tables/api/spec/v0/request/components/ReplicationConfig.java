package com.linkedin.openhouse.tables.api.spec.v0.request.components;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.Valid;
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
public class ReplicationConfig {
  @Schema(description = "Replication destination cluster name", example = "clusterA")
  @NotNull(
      message =
          "Incorrect destination specified. Destination field for replication config cannot be null")
  @Valid
  String destination;

  @Schema(
      description =
          "Optional parameter interval at which the replication job should run. Default value is 1D",
      example = "1D")
  @Valid
  String interval;

  @Schema(
      description = "Cron schedule generated from interval used for replication job scheduling",
      example = "Schedule could be '0 3/12 * * *' if interval is 12H")
  @Valid
  String cronSchedule;

  public enum Granularity {
    H,
    D
  }
}
