package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.Arrays;
import java.util.List;

/**
 * A task to remove orphan files from a table.
 *
 * @see <a href="https://iceberg.apache.org/docs/latest/maintenance/#delete-orphan-files">Delete
 *     orphan files</a>
 */
public class TableOrphanFilesDeletionTask extends TableOperationTask<TableMetadata> {
  public static final JobConf.JobTypeEnum OPERATION_TYPE =
      JobConf.JobTypeEnum.ORPHAN_FILES_DELETION;

  public TableOrphanFilesDeletionTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      TableMetadata metadata,
      long pollIntervalMs,
      long timeoutMs) {
    super(jobsClient, tablesClient, metadata, pollIntervalMs, timeoutMs);
  }

  protected TableOrphanFilesDeletionTask(
      JobsClient jobsClient, TablesClient tablesClient, TableMetadata metadata) {
    super(jobsClient, tablesClient, metadata);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected List<String> getArgs() {
    return Arrays.asList("--tableName", metadata.fqtn());
  }

  @Override
  protected boolean shouldRun() {
    return true;
  }
}
