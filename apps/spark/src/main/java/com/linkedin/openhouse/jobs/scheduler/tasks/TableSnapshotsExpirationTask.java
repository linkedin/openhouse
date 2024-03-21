package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.Arrays;
import java.util.List;

/**
 * A task to expire snapshots from a table.
 *
 * @see <a href="https://iceberg.apache.org/docs/latest/maintenance/#expire-snapshots">Expire
 *     snapshots</a>
 */
public class TableSnapshotsExpirationTask extends TableOperationTask {
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
    return Arrays.asList(
        "--tableName", getMetadata().fqtn(),
        "--granularity", "days",
        "--count", "3");
  }

  @Override
  protected boolean shouldRun() {
    return tablesClient.canExpireSnapshots(getMetadata());
  }
}
