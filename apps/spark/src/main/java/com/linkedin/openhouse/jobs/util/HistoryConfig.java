package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.tables.client.model.Retention;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Table retention config class. This is app side representation of /tables policies->retention */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class HistoryConfig {
  private final int maxAge;
  private final int minVersions;
  private final Retention.GranularityEnum granularity;
}
