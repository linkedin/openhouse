package com.linkedin.openhouse.common.stats.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/** Data Model for capturing retention stats about a table. */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder(toBuilder = true)
public class RetentionStatsSchema {

  private Integer count;

  private String granularity;

  private String columnName;

  private String columnPattern;
}
