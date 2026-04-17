package com.linkedin.openhouse.common.stats.model;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PolicyStats {
  Boolean sharingEnabled;
  RetentionStatsSchema retentionPolicy;
  HistoryPolicyStatsSchema historyPolicy;
  List<ReplicationPolicyStatsSchema> replicationPolicies;

  public PolicyStats() {
    this.sharingEnabled = sharingEnabled;
    this.retentionPolicy = retentionPolicy;
    this.historyPolicy = historyPolicy;
    this.replicationPolicies = replicationPolicies;
  }
}
