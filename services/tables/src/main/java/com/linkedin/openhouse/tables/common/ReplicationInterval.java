package com.linkedin.openhouse.tables.common;

/**
 * ENUM for default replication interval associated with Interval in {@link
 * com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig}
 */
public enum ReplicationInterval {
  // default interval to run replication jobs if no interval provided by user
  DEFAULT("1D");

  private final String interval;

  ReplicationInterval(String interval) {
    this.interval = interval;
  }

  public String getInterval() {
    return interval;
  }
}
