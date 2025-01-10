package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.HistoryConfig;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A task to expire snapshots from a table.
 *
 * @see <a href="https://iceberg.apache.org/docs/latest/maintenance/#expire-snapshots">Expire
 *     snapshots</a>
 */
public class TableSnapshotsExpirationTask extends TableOperationTask<TableMetadata> {
  public static final JobConf.JobTypeEnum OPERATION_TYPE = JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION;

  protected TableSnapshotsExpirationTask(
      JobsClient jobsClient, TablesClient tablesClient, TableMetadata tableMetadata) {
    super(jobsClient, tablesClient, tableMetadata);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected List<String> getArgs() {
    HistoryConfig config = metadata.getHistoryConfig();
    List<String> jobArgs = new ArrayList<>();
    if (config.getMaxAge() > 0) {
      jobArgs.addAll(
          Arrays.asList(
              "--maxAge", Integer.toString(config.getMaxAge()),
              "--granularity", config.getGranularity().getValue()));
    }
    if (config.getVersions() > 0) {
      jobArgs.addAll(Arrays.asList("--versions", Integer.toString(config.getVersions())));
    }
    return jobArgs;
  }

  @Override
  protected boolean shouldRun() {
    return metadata.isPrimary();
  }
}
