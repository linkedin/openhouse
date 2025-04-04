package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.datalayout.persistence.StrategiesDaoTableProps;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.TableDataLayoutMetadata;
import java.util.ArrayList;
import java.util.List;

public class TableDataLayoutPartitionStrategyExecutionTask
    extends TableOperationTask<TableDataLayoutMetadata> {
  public static final JobConf.JobTypeEnum OPERATION_TYPE =
      JobConf.JobTypeEnum.DATA_LAYOUT_PARTITION_STRATEGY_EXECUTION;

  public TableDataLayoutPartitionStrategyExecutionTask(
      JobsClient jobsClient,
      TablesClient tablesClient,
      TableDataLayoutMetadata metadata,
      long pollIntervalMs,
      long timeoutMs) {
    super(jobsClient, tablesClient, metadata, pollIntervalMs, timeoutMs);
  }

  public TableDataLayoutPartitionStrategyExecutionTask(
      JobsClient jobsClient, TablesClient tablesClient, TableDataLayoutMetadata metadata) {
    super(jobsClient, tablesClient, metadata);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected List<String> getArgs() {
    String strategies = StrategiesDaoTableProps.serialize(metadata.getDataLayoutStrategies());
    List<String> args = new ArrayList<>();
    args.add("--tableName");
    args.add(metadata.fqtn());
    args.add("--strategies");
    args.add(strategies);
    return args;
  }

  @Override
  protected boolean shouldRun() {
    return metadata.isPrimary();
  }
}
