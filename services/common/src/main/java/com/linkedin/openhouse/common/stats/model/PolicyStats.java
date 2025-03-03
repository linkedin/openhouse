package com.linkedin.openhouse.common.stats.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PolicyStats {
  Boolean sharingEnabled;
  RetentionStatsSchema retentionPolicy;
  HistoryPolicyStatsSchema historyPolicy;
}
