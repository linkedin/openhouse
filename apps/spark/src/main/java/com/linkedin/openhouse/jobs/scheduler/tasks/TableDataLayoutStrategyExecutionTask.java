package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.NotImplementedException;

public class TableDataLayoutStrategyExecutionTask extends TableOperationTask {
  // FIXME
  public static final JobConf.JobTypeEnum OPERATION_TYPE =
      JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_GENERATION;

  protected TableDataLayoutStrategyExecutionTask(
      JobsClient jobsClient, TablesClient tablesClient, TableMetadata tableMetadata) {
    super(jobsClient, tablesClient, tableMetadata);
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
    throw new NotImplementedException("Not implemented");
  }
}
