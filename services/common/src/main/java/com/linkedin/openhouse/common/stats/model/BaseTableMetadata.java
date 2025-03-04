package com.linkedin.openhouse.common.stats.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/** Data Model for capturing table metadata for stats. */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder(toBuilder = true)
public class BaseTableMetadata {

  private Long recordTimestamp;

  private String clusterName;

  private String databaseName;

  private String tableName;

  private String tableUUID;

  private String tableLocation;

  private String tableCreator;

  private Long tableCreationTimestamp;

  private Long tableLastUpdatedTimestamp;

  private String tableType;

  private Boolean sharingEnabled;

  private RetentionStatsSchema retentionPolicies;

  private HistoryPolicyStatsSchema historyPolicy;
}
