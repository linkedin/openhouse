package com.linkedin.openhouse.jobs.util;

import com.linkedin.openhouse.jobs.client.model.JobConf;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/** Database metadata */
@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class DatabaseMetadata extends Metadata {
  protected @NonNull String dbName;
  @Builder.Default protected @NonNull Map<String, String> jobExecutionProperties = new HashMap<>();

  public boolean isMaintenanceJobDisabled(JobConf.JobTypeEnum jobType) {
    return Boolean.parseBoolean(jobExecutionProperties.get("disabled"))
        || Boolean.parseBoolean(jobExecutionProperties.get(String.format("%s.disabled", jobType)));
  }

  @Override
  public String getEntityName() {
    return dbName;
  }
}
