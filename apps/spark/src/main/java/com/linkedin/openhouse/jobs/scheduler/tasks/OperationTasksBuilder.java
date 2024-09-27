package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.datalayout.ranker.DataLayoutCandidateSelector;
import com.linkedin.openhouse.datalayout.ranker.DataLayoutStrategyScorer;
import com.linkedin.openhouse.datalayout.ranker.GreedyMaxBudgetCandidateSelector;
import com.linkedin.openhouse.datalayout.ranker.SimpleWeightedSumDataLayoutStrategyScorer;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.datalayout.strategy.ScoredDataLayoutStrategy;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.DirectoryMetadata;
import com.linkedin.openhouse.jobs.util.Metadata;
import com.linkedin.openhouse.jobs.util.TableDataLayoutMetadata;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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
  private final OperationTaskFactory<? extends OperationTask<?>> taskFactory;

  protected final TablesClient tablesClient;

  private List<OperationTask<?>> prepareTableOperationTaskList(JobConf.JobTypeEnum jobType) {
    List<TableMetadata> tableMetadataList = tablesClient.getTableMetadataList();
    log.info("Fetched metadata for {} tables", tableMetadataList.size());
    return processMetadataList(tableMetadataList, jobType);
  }

  private List<OperationTask<?>> prepareTableDirectoryOperationTaskList(
      JobConf.JobTypeEnum jobType) {
    List<DirectoryMetadata> directoryMetadataList = tablesClient.getOrphanTableDirectories();
    log.info("Fetched metadata for {} directories", directoryMetadataList.size());
    return processMetadataList(directoryMetadataList, jobType);
  }

  private List<OperationTask<?>> prepareDataLayoutOperationTaskList(JobConf.JobTypeEnum jobType) {
    List<TableDataLayoutMetadata> tableDataLayoutMetadataList =
        tablesClient.getTableDataLayoutMetadataList();
    log.info("Fetched metadata for {} data layout strategies", tableDataLayoutMetadataList.size());
    List<DataLayoutStrategy> strategies =
        tableDataLayoutMetadataList.stream()
            .map(TableDataLayoutMetadata::getDataLayoutStrategy)
            .collect(Collectors.toList());
    DataLayoutStrategyScorer scorer = new SimpleWeightedSumDataLayoutStrategyScorer(0.5, 0.5);
    List<ScoredDataLayoutStrategy> scoredStrategies = scorer.scoreDataLayoutStrategies(strategies);
    DataLayoutCandidateSelector candidateSelector =
        new GreedyMaxBudgetCandidateSelector(1000.0, 10);
    List<Integer> selectedStrategyIndices = candidateSelector.select(scoredStrategies);
    log.info("Selected {} strategies", selectedStrategyIndices.size());
    List<TableDataLayoutMetadata> selectedTableDataLayoutMetadataList =
        selectedStrategyIndices.stream()
            .map(tableDataLayoutMetadataList::get)
            .collect(Collectors.toList());
    return processMetadataList(selectedTableDataLayoutMetadataList, jobType);
  }

  private List<OperationTask<?>> processMetadataList(
      List<? extends Metadata> metadataList, JobConf.JobTypeEnum jobType) {
    List<OperationTask<?>> taskList = new ArrayList<>();
    for (Metadata metadata : metadataList) {
      log.info("Found metadata {}", metadata);
      try {
        OperationTask<?> task = taskFactory.create(metadata);
        if (!task.shouldRun()) {
          log.info("Skipping task {}", task);
        } else {
          taskList.add(task);
        }
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(
                "Cannot create operation task metadata %s given job type: %s", metadata, jobType),
            e);
      }
    }
    return taskList;
  }

  // Method to build a task list based on the operation task type
  public List<OperationTask<?>> buildOperationTaskList(JobConf.JobTypeEnum jobType) {
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
      case DATA_LAYOUT_STRATEGY_EXECUTION:
        return prepareDataLayoutOperationTaskList(jobType);
      case ORPHAN_DIRECTORY_DELETION:
        return prepareTableDirectoryOperationTaskList(jobType);
      default:
        throw new UnsupportedOperationException(
            String.format("Job type %s is not supported", jobType));
    }
  }
}
