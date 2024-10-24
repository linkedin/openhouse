package com.linkedin.openhouse.tables.common;

import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;

/** ENUM for default replication interval associated with Interval in {@link ReplicationConfig} */
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
