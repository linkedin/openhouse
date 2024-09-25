package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/** Table metadata, including database name, table name and owner. */
@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString
public class TableMetadata extends Metadata {
  protected String dbName;
  protected String tableName;
  protected long creationTimeMs;
  protected boolean isPrimary;
  protected boolean isTimePartitioned;
  protected boolean isClustered;
  protected @Nullable RetentionConfig retentionConfig;
  @Builder.Default protected List<DataLayoutStrategy> dataLayoutStrategies = new ArrayList<>();

  public String fqtn() {
    return String.format("%s.%s", dbName, tableName);
  }

  @Override
  public String getEntityName() {
    return fqtn();
  }
}
