package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.exception.OperationTaskException;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableDataCompactionTask extends TableOperationTask {
  public static final JobConf.JobTypeEnum OPERATION_TYPE = JobConf.JobTypeEnum.DATA_COMPACTION;

  protected TableDataCompactionTask(
      JobsClient jobsClient, TablesClient tablesClient, TableMetadata tableMetadata) {
    super(jobsClient, tablesClient, tableMetadata);
  }

  @Override
  public JobConf.JobTypeEnum getType() {
    return OPERATION_TYPE;
  }

  @Override
  protected List<String> getArgs() throws OperationTaskException {
    TableMetadata tableMetadata = getMetadata();
    Optional<Long> targetSize = tablesClient.getTableDataFileTargetSizeBytes(tableMetadata);
    if (!targetSize.isPresent()) {
      throw new OperationTaskException(
          "Couldn't construct task arguments: couldn't fetch target file size");
    }
    // only consider very small (< 15% of target size) files for compaction
    long minSize = (long) (targetSize.get() * 0.15);
    long maxSize = (long) (targetSize.get() * 1.8);
    return Stream.of(
            "--tableName",
            tableMetadata.fqtn(),
            "--partialProgressEnabled",
            "--partialProgressMaxCommits",
            "50",
            "--maxConcurrentFileGroupRewrites",
            "300",
            "--targetByteSize",
            targetSize.get().toString(),
            "--minByteSize",
            Long.toString(minSize),
            "--maxByteSize",
            Long.toString(maxSize))
        .collect(Collectors.toList());
  }

  @Override
  protected boolean shouldRun() {
    return true;
  }
}
