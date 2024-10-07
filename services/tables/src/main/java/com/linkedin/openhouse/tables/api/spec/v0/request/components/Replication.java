package com.linkedin.openhouse.tables.api.spec.v0.request.components;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
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
public class Replication {
  @Schema(
      description = "List of replication Schedules for replication flow setup.",
      example = "clusterA: 0 0 1 * * ?, clusterB: 0 0 1 * * ?")
  @NotNull(message = "Incorrect replication policy specified. Replication spec cannot be null.")
  @Valid
  List<Schedule> schedules;
}
