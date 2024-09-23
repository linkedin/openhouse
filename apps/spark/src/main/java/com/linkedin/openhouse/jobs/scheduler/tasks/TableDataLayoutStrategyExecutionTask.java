package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.datalayout.persistence.StrategiesDaoTableProps;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.TableDataLayoutMetadata;
import java.util.Arrays;
import java.util.List;

public class TableDataLayoutStrategyExecutionTask
    extends TableOperationTask<TableDataLayoutMetadata> {
  public static final JobConf.JobTypeEnum OPERATION_TYPE =
      JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_EXECUTION;

  protected TableDataLayoutStrategyExecutionTask(
      JobsClient jobsClient, TablesClient tablesClient, TableDataLayoutMetadata metadata) {
    super(jobsClient, tablesClient, metadata);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected List<String> getArgs() {
    return Arrays.asList(
        "--tableName",
        metadata.fqtn(),
        "--config",
        StrategiesDaoTableProps.serialize(metadata.getDataLayoutStrategy()));
  }

  @Override
  protected boolean shouldRun() {
    return metadata.isPrimary() && (metadata.isTimePartitioned() || metadata.isClustered());
  }
}
