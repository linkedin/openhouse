package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.tables.client.model.Retention;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** History config class. This is app side representation of /tables policies->history */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class HistoryConfig {
  private final Retention.GranularityEnum granularity;
  private final int maxAge;
  private final int versions;
}
