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
import io.opentelemetry.api.metrics.Meter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;

/**
 * Prepares the task list based on the job type. Right now task type is either table based or
 * directory based
 */
@Slf4j
@AllArgsConstructor
public class OperationTasksBuilder {
  public static final String MAX_COST_BUDGET_GB_HRS = "maxCostBudgetGbHrs";
  public static final String MAX_STRATEGIES_COUNT = "maxStrategiesCount";
  public static final String MAX_COMPUTE_COST_PER_TABLE = "maxComputeCostPerTable";
  private static final double COMPUTE_COST_WEIGHT_DEFAULT = 0.3;
  private static final double COMPACTION_GAIN_WEIGHT_DEFAULT = 0.7;
  private static final double MAX_COST_BUDGET_GB_HRS_DEFAULT = 1000.0;
  private static final int MAX_STRATEGIES_COUNT_DEFAULT = 10;
  private static final int MAX_COMPUTE_COST_PER_TABLE_DEFAULT = 100; // 24 hrs

  @Getter(AccessLevel.NONE)
  private final OperationTaskFactory<? extends OperationTask<?>> taskFactory;

  protected final TablesClient tablesClient;

  private List<OperationTask<?>> prepareTableOperationTaskList(JobConf.JobTypeEnum jobType) {
    List<TableMetadata> tableMetadataList = tablesClient.getTableMetadataList();
    log.info("Fetched metadata for {} tables", tableMetadataList.size());
    return processMetadataList(tableMetadataList, jobType);
  }

  private List<OperationTask<?>> prepareReplicationOperationTaskList(JobConf.JobTypeEnum jobType) {
    List<TableMetadata> replicationSetupTableMetadataList = tablesClient.getTableMetadataList();
    // filters tables which are primary and hava replication config defined
    replicationSetupTableMetadataList =
        replicationSetupTableMetadataList.stream()
            .filter(m -> m.isPrimary() && (m.getReplicationConfig() != null))
            .collect(Collectors.toList());
    log.info(
        "Fetched metadata for {} tables for replication setup task",
        replicationSetupTableMetadataList.size());
    return processMetadataList(replicationSetupTableMetadataList, jobType);
  }

  private List<OperationTask<?>> prepareTableDirectoryOperationTaskList(
      JobConf.JobTypeEnum jobType) {
    List<DirectoryMetadata> directoryMetadataList = tablesClient.getOrphanTableDirectories();
    log.info("Fetched metadata for {} directories", directoryMetadataList.size());
    return processMetadataList(directoryMetadataList, jobType);
  }

  /** Rank and select the strategies for data layout execution. */
  private List<OperationTask<?>> prepareDataLayoutOperationTaskList(
      JobConf.JobTypeEnum jobType, Properties properties, Meter meter, boolean isPartitionScope) {
    List<TableDataLayoutMetadata> tableDataLayoutMetadataList =
        tablesClient.getTableDataLayoutMetadataList(isPartitionScope);
    // filter out non-primary and non-clustered/time-partitioned tables before ranking
    tableDataLayoutMetadataList =
        tableDataLayoutMetadataList.stream()
            .filter(TableMetadata::isPrimary)
            .collect(Collectors.toList());
    log.info("Fetched metadata for {} data layout strategies", tableDataLayoutMetadataList.size());
    List<DataLayoutStrategy> strategies =
        tableDataLayoutMetadataList.stream()
            .map(metadata -> metadata.getDataLayoutStrategies().get(0))
            // filter out strategies with no gain/file count reduction
            // or discounted to 0, e.g. frequently overwritten un-partitioned tables
            .filter(s -> s.getGain() * (1.0 - s.getFileCountReductionPenalty()) >= 1.0)
            .collect(Collectors.toList());
    log.info("Collected {} strategies for ranking", strategies.size());
    DataLayoutStrategyScorer scorer =
        new SimpleWeightedSumDataLayoutStrategyScorer(
            COMPACTION_GAIN_WEIGHT_DEFAULT, COMPUTE_COST_WEIGHT_DEFAULT);
    List<ScoredDataLayoutStrategy> scoredStrategies = scorer.scoreDataLayoutStrategies(strategies);
    double maxComputeCost =
        NumberUtils.toDouble(
            properties.getProperty(MAX_COST_BUDGET_GB_HRS), MAX_COST_BUDGET_GB_HRS_DEFAULT);
    int maxStrategiesCount =
        NumberUtils.toInt(
            properties.getProperty(MAX_STRATEGIES_COUNT), MAX_STRATEGIES_COUNT_DEFAULT);
    double maxComputeCostPerTable =
        NumberUtils.toDouble(
            properties.getProperty(MAX_COMPUTE_COST_PER_TABLE), MAX_COMPUTE_COST_PER_TABLE_DEFAULT);
    log.info(
        "Max compute cost budget: {}, max strategies count: {}",
        maxComputeCost,
        maxStrategiesCount);
    DataLayoutCandidateSelector candidateSelector =
        new GreedyMaxBudgetCandidateSelector(
            maxComputeCost, maxStrategiesCount, maxComputeCostPerTable, isPartitionScope);
    List<Integer> selectedStrategyIndices = candidateSelector.select(scoredStrategies);
    log.info("Selected {} strategies", selectedStrategyIndices.size());
    List<TableDataLayoutMetadata> selectedTableDataLayoutMetadataList =
        selectedStrategyIndices.stream()
            .map(tableDataLayoutMetadataList::get)
            .collect(Collectors.toList());
    double totalComputeCost =
        selectedTableDataLayoutMetadataList.stream()
            .map(m -> m.getDataLayoutStrategies().get(0).getCost())
            .reduce(0.0, Double::sum);
    double totalReducedFileCount =
        selectedTableDataLayoutMetadataList.stream()
            .map(m -> m.getDataLayoutStrategies().get(0).getGain())
            .reduce(0.0, Double::sum);
    log.info(
        "Total estimated compute cost: {}, total estimated reduced file count: {}",
        totalComputeCost,
        totalReducedFileCount);
    meter
        .counterBuilder("data_layout_optimization_estimated_compute_cost")
        .build()
        .add((long) totalComputeCost);
    meter
        .counterBuilder("data_layout_optimization_estimated_reduced_file_count")
        .build()
        .add((long) totalReducedFileCount);
    if (isPartitionScope) {
      // Aggregate the strategies by table
      return processMetadataList(
          aggregateTableDataLayoutMetadata(selectedTableDataLayoutMetadataList), jobType);
    } else {
      return processMetadataList(selectedTableDataLayoutMetadataList, jobType);
    }
  }

  private List<TableDataLayoutMetadata> aggregateTableDataLayoutMetadata(
      List<TableDataLayoutMetadata> selectedTableDataLayoutMetadataList) {
    Map<String, List<TableDataLayoutMetadata>> groupedTableDataLayoutMetadataMap =
        selectedTableDataLayoutMetadataList.stream()
            .collect(Collectors.groupingBy(TableDataLayoutMetadata::fqtn));
    List<TableDataLayoutMetadata> aggregatedTableDataLayoutMetadataList = new ArrayList<>();
    for (List<TableDataLayoutMetadata> metadataListPerTable :
        groupedTableDataLayoutMetadataMap.values()) {
      List<DataLayoutStrategy> aggregatedStrategies = new ArrayList<>();
      metadataListPerTable.stream()
          .map(TableDataLayoutMetadata::getDataLayoutStrategies)
          .forEach(aggregatedStrategies::addAll);
      aggregatedTableDataLayoutMetadataList.add(
          TableDataLayoutMetadata.from(metadataListPerTable.get(0), aggregatedStrategies));
    }
    return aggregatedTableDataLayoutMetadataList;
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

  /**
   * Fetches tables and associated metadata from Tables Service, and builds the operation task list.
   */
  public List<OperationTask<?>> buildOperationTaskList(
      JobConf.JobTypeEnum jobType, Properties properties, Meter meter) {
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
      case REPLICATION:
        return prepareReplicationOperationTaskList(jobType);
      case DATA_LAYOUT_STRATEGY_EXECUTION:
        return prepareDataLayoutOperationTaskList(jobType, properties, meter, false);
      case DATA_LAYOUT_PARTITION_STRATEGY_EXECUTION:
        return prepareDataLayoutOperationTaskList(jobType, properties, meter, true);
      case ORPHAN_DIRECTORY_DELETION:
        return prepareTableDirectoryOperationTaskList(jobType);
      default:
        throw new UnsupportedOperationException(
            String.format("Job type %s is not supported", jobType));
    }
  }
}
