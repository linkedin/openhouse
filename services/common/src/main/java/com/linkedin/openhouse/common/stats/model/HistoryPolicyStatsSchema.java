package com.linkedin.openhouse.common.stats.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder(toBuilder = true)
public class HistoryPolicyStatsSchema {

  private Integer numVersions;

  private Long expectedEarliestSnapshotTimestampMillis;

  private Integer maxAge;

  private String dateGranularity;
}
