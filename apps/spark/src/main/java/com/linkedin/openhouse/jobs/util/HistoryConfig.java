package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.tables.client.model.History;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** History Policy config class. This is app side representation of /tables policies->history */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class HistoryConfig {
  private final int maxAge;
  private final int versions;
  private final History.GranularityEnum granularity;
}
