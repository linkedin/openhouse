package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.HistoryConfig;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A task to expire snapshots from a table.
 *
 * @see <a href="https://iceberg.apache.org/docs/latest/maintenance/#expire-snapshots">Expire
 *     snapshots</a>
 */
public class TableSnapshotsExpirationTask extends TableOperationTask<TableMetadata> {
  public static final JobConf.JobTypeEnum OPERATION_TYPE = JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION;

  private final boolean deleteFiles;

  public TableSnapshotsExpirationTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      TableMetadata metadata,
      long pollIntervalMs,
      long queuedTimeoutMs,
      long taskTimeoutMs,
      boolean deleteFiles) {
    super(jobsClient, tablesClient, metadata, pollIntervalMs, queuedTimeoutMs, taskTimeoutMs);
    this.deleteFiles = deleteFiles;
  }

  public TableSnapshotsExpirationTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      TableMetadata metadata,
      boolean deleteFiles) {
    super(jobsClient, tablesClient, metadata);
    this.deleteFiles = deleteFiles;
  }

  // Backward compatibility constructor
  public TableSnapshotsExpirationTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      TableMetadata metadata,
      long pollIntervalMs,
      long queuedTimeoutMs,
      long taskTimeoutMs) {
    this(jobsClient, tablesClient, metadata, pollIntervalMs, queuedTimeoutMs, taskTimeoutMs, false);
  }

  // Backward compatibility constructor
  public TableSnapshotsExpirationTask(
      JobsClient jobsClient, TablesClient tablesClient, TableMetadata metadata) {
    this(jobsClient, tablesClient, metadata, false);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected List<String> getArgs() {
    HistoryConfig config = metadata.getHistoryConfig();
    List<String> jobArgs = new ArrayList<>(Arrays.asList("--tableName", metadata.fqtn()));
    if (config != null) {
      if (config.getMaxAge() > 0) {
        jobArgs.addAll(
            Arrays.asList(
                "--maxAge",
                Objects.toString(config.getMaxAge()),
                "--granularity",
                config.getGranularity().getValue()));
      }
      if (config.getVersions() > 0) {
        jobArgs.addAll(Arrays.asList("--versions", Objects.toString(config.getVersions())));
      }
    }
    // Add deleteFiles flag if enabled
    if (deleteFiles) {
      jobArgs.add("--deleteFiles");
    }
    return jobArgs;
  }

  @Override
  protected boolean shouldRunTask() {
    return metadata.isPrimary();
  }
}
