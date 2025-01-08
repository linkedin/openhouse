package com.linkedin.openhouse.jobs.util;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Table retention config class. This is app side representation of /tables policies->retention */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class ReplicationConfig {
  private final String schedule;
  private final String proxyUser;
  private final String cluster;
}
