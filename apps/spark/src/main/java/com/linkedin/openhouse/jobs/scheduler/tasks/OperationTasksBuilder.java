package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.DirectoryMetadata;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Prepares the task list based on the job type. Right now task type is either table based or
 * directory based
 */
@Slf4j
@AllArgsConstructor
public class OperationTasksBuilder {
  @Getter(AccessLevel.NONE)
  private final OperationTaskFactory<? extends OperationTask> taskFactory;

  protected final TablesClient tablesClient;

  private List<OperationTask> prepareTableOperationTaskList(JobConf.JobTypeEnum jobType) {
    List<OperationTask> taskList = new ArrayList<>();
    for (TableMetadata metadata : tablesClient.getTableMetadataList()) {
      log.info("metadata: {}", metadata);
      try {
        taskList.add(taskFactory.create(metadata));
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(
                "Cannot create operation task from TableMetadata: %s given job type: %s",
                metadata, jobType),
            e);
      }
    }
    return taskList;
  }

  private List<OperationTask> prepareTableDirectoryOperationTaskList(JobConf.JobTypeEnum jobType) {
    List<OperationTask> taskList = new ArrayList<>();
    for (DirectoryMetadata metadata : tablesClient.getOrphanTableDirectories()) {
      log.info("metadata: {}", metadata);
      try {
        taskList.add(taskFactory.create(metadata));
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(
                "Cannot create operation task from DirectoryMetadata: %s given job type: %s",
                metadata, jobType),
            e);
      }
    }
    return taskList;
  }

  // Method to build a task list based on the operation task type
  public List<OperationTask> buildOperationTaskList(JobConf.JobTypeEnum jobType) {
    switch (jobType) {
      case DATA_COMPACTION:
      case NO_OP:
      case ORPHAN_FILES_DELETION:
      case RETENTION:
      case SNAPSHOTS_EXPIRATION:
      case SQL_TEST:
      case TABLE_STATS_COLLECTION:
      case STAGED_FILES_DELETION:
      case DATA_LAYOUT_STRATEGY_GENERATION:
        return prepareTableOperationTaskList(jobType);
      case ORPHAN_DIRECTORY_DELETION:
        return prepareTableDirectoryOperationTaskList(jobType);
      default:
        throw new UnsupportedOperationException(
            String.format("Job type %s is not supported", jobType));
    }
  }
}
