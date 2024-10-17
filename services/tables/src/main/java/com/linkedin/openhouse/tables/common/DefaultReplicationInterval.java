package com.linkedin.openhouse.tables.common;

/**
 * ENUM for default replication interval associated with Interval in {@link
 * com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig}
 */
public enum DefaultReplicationInterval {
  // default interval to run replication jobs
  DAILY("24H");

  private final String interval;

  DefaultReplicationInterval(String interval) {
    this.interval = interval;
  }

  public String getInterval() {
    return interval;
  }
}
