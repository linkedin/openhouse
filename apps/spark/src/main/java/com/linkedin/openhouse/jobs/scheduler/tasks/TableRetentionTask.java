package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.RetentionConfig;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang.StringUtils;

/** A task to apply retention to a table. */
public class TableRetentionTask extends TableOperationTask<TableMetadata> {
  public static final JobConf.JobTypeEnum OPERATION_TYPE = JobConf.JobTypeEnum.RETENTION;

  public TableRetentionTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      TableMetadata metadata,
      long pollIntervalMs,
      long queuedTimeoutMs,
      long taskTimeoutMs) {
    super(jobsClient, tablesClient, metadata, pollIntervalMs, queuedTimeoutMs, taskTimeoutMs);
  }

  public TableRetentionTask(
      JobsClient jobsClient, TablesClient tablesClient, TableMetadata metadata) {
    super(jobsClient, tablesClient, metadata);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected List<String> getArgs() {
    RetentionConfig config = metadata.getRetentionConfig();
    String columnName = Objects.requireNonNull(config).getColumnName();
    List<String> jobArgs =
        Stream.of(
                "--tableName",
                metadata.fqtn(),
                "--columnName",
                columnName,
                "--granularity",
                config.getGranularity().getValue(),
                "--count",
                Integer.toString(config.getCount()))
            .collect(Collectors.toList());

    if (!StringUtils.isBlank(config.getColumnPattern())) {
      jobArgs.add("--columnPattern");
      jobArgs.add(config.getColumnPattern());
    }
    return jobArgs;
  }

  @Override
  protected boolean shouldRunTask() {
    return metadata.isPrimary() && metadata.getRetentionConfig() != null;
  }
}
