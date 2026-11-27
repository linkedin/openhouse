package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.jobs.client.model.JobConf;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/** Table metadata, including database name, table name and owner. */
@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class TableMetadata extends Metadata {
  private static final EnumSet<JobConf.JobTypeEnum> TEMPORARY_TABLE_MAINTENANCE =
      EnumSet.of(
          JobConf.JobTypeEnum.DATA_COMPACTION,
          JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_GENERATION,
          JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_EXECUTION,
          JobConf.JobTypeEnum.RETENTION,
          JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION,
          JobConf.JobTypeEnum.ORPHAN_FILES_DELETION,
          JobConf.JobTypeEnum.ORPHAN_FILES_DELETION_BATCH,
          JobConf.JobTypeEnum.STAGED_FILES_DELETION);

  protected @NonNull String dbName;
  protected @NonNull String tableName;
  protected long creationTimeMs;
  protected boolean isPrimary;
  protected boolean isTimePartitioned;
  protected boolean isClustered;
  protected boolean isTemporary;
  @Builder.Default protected @NonNull Map<String, String> jobExecutionProperties = new HashMap<>();
  protected @Nullable RetentionConfig retentionConfig;
  protected @Nullable HistoryConfig historyConfig;
  protected @Nullable List<ReplicationConfig> replicationConfig;

  public boolean isMaintenanceJobDisabled(JobConf.JobTypeEnum jobType) {
    return (isTemporary && TEMPORARY_TABLE_MAINTENANCE.contains(jobType))
        || Boolean.parseBoolean(jobExecutionProperties.get("disabled"))
        || Boolean.parseBoolean(jobExecutionProperties.get(String.format("%s.disabled", jobType)));
  }

  public String fqtn() {
    return String.format("%s.%s", dbName, tableName);
  }

  @Override
  public String getEntityName() {
    return fqtn();
  }
}
