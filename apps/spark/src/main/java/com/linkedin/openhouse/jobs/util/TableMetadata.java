package com.linkedin.openhouse.jobs.util;

import java.util.HashMap;
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
  protected @NonNull String dbName;
  protected @NonNull String tableName;
  protected long creationTimeMs;
  protected boolean isPrimary;
  protected boolean isTimePartitioned;
  protected boolean isClustered;
  @Builder.Default protected @NonNull Map<String, String> jobExecutionProperties = new HashMap<>();
  protected @Nullable RetentionConfig retentionConfig;
  protected @Nullable HistoryConfig historyConfig;

  public String fqtn() {
    return String.format("%s.%s", dbName, tableName);
  }

  @Override
  public String getEntityName() {
    return fqtn();
  }
}
