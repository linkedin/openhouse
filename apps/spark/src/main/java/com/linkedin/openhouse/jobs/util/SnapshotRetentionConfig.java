package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.tables.client.model.Retention;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Snapshots retention config class. This is app side representation of /tables
 * policies->snapshotRetention
 */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class SnapshotRetentionConfig {
  private final String columnName;
  private final String columnPattern;
  private final Retention.GranularityEnum granularity;
  private final int count;
}
