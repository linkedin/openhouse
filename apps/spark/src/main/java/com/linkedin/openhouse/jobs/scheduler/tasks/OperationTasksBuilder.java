package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.openhouse.datalayout.ranker.DataLayoutCandidateSelector;
import com.linkedin.openhouse.datalayout.ranker.DataLayoutStrategyScorer;
import com.linkedin.openhouse.datalayout.ranker.GreedyMaxBudgetCandidateSelector;
import com.linkedin.openhouse.datalayout.ranker.SimpleWeightedSumDataLayoutStrategyScorer;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.DataLayoutUtil;
import com.linkedin.openhouse.jobs.util.DirectoryMetadata;
import com.linkedin.openhouse.jobs.util.Metadata;
import com.linkedin.openhouse.jobs.util.TableDataLayoutMetadata;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import io.opentelemetry.api.metrics.Meter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Prepares the task list based on the job type. Right now task type is either table based or
 * directory based
 */
@Slf4j
@AllArgsConstructor
public class OperationTasksBuilder {
  public static final String MAX_COST_BUDGET_GB_HRS = "maxCostBudgetGbHrs";
  public static final String MAX_STRATEGIES_COUNT = "maxStrategiesCount";
  private static final double COMPUTE_COST_WEIGHT_DEFAULT = 0.3;
  private static final double COMPACTION_GAIN_WEIGHT_DEFAULT = 0.7;
  private static final double MAX_COST_BUDGET_GB_HRS_DEFAULT = 1000.0;
  private static final int MAX_STRATEGIES_COUNT_DEFAULT = 10;

  @Getter(AccessLevel.NONE)
  private final OperationTaskFactory<? extends OperationTask<?>> taskFactory;

  protected final TablesClient tablesClient;

  private final int numParallelMetadataFetch;
  private final OperationTaskManager operationTaskManager;
  private final JobInfoManager jobInfoManager;

  private List<OperationTask<?>> prepareTableOperationTaskList(
      JobConf.JobTypeEnum jobType, OperationMode operationMode) {
    List<TableMetadata> tableMetadataList = tablesClient.getTableMetadataList();
    log.info("Fetched metadata for {} tables", tableMetadataList.size());
    return processMetadataList(tableMetadataList, jobType, operationMode);
  }

  private List<OperationTask<?>> prepareReplicationOperationTaskList(
      JobConf.JobTypeEnum jobType, OperationMode operationMode) {
    List<TableMetadata> replicationSetupTableMetadataList = tablesClient.getTableMetadataList();
    // filters tables which are primary and hava replication config defined
    replicationSetupTableMetadataList =
        replicationSetupTableMetadataList.stream()
            .filter(m -> m.isPrimary() && (m.getReplicationConfig() != null))
            .collect(Collectors.toList());
    log.info(
        "Fetched metadata for {} tables for replication setup task",
        replicationSetupTableMetadataList.size());
    return processMetadataList(replicationSetupTableMetadataList, jobType, operationMode);
  }

  private List<OperationTask<?>> prepareTableDirectoryOperationTaskList(
      JobConf.JobTypeEnum jobType, OperationMode operationMode) {
    List<DirectoryMetadata> directoryMetadataList = tablesClient.getOrphanTableDirectories();
    log.info("Fetched metadata for {} directories", directoryMetadataList.size());
    return processMetadataList(directoryMetadataList, jobType, operationMode);
  }

  private List<OperationTask<?>> prepareDataLayoutOperationTaskList(
      JobConf.JobTypeEnum jobType,
      Properties properties,
      Meter meter,
      OperationMode operationMode) {
    List<TableDataLayoutMetadata> tableDataLayoutMetadataList =
        tablesClient.getTableDataLayoutMetadataList();
    DataLayoutStrategyScorer scorer =
        new SimpleWeightedSumDataLayoutStrategyScorer(
            COMPACTION_GAIN_WEIGHT_DEFAULT, COMPUTE_COST_WEIGHT_DEFAULT);
    double maxComputeCost =
        NumberUtils.toDouble(
            properties.getProperty(MAX_COST_BUDGET_GB_HRS), MAX_COST_BUDGET_GB_HRS_DEFAULT);
    int maxStrategiesCount =
        NumberUtils.toInt(
            properties.getProperty(MAX_STRATEGIES_COUNT), MAX_STRATEGIES_COUNT_DEFAULT);
    log.info(
        "Max compute cost budget: {}, max strategies count: {}",
        maxComputeCost,
        maxStrategiesCount);
    DataLayoutCandidateSelector candidateSelector =
        new GreedyMaxBudgetCandidateSelector(maxComputeCost, maxStrategiesCount);
    List<TableDataLayoutMetadata> selectedTableDataLayoutMetadataList =
        DataLayoutUtil.selectStrategies(scorer, candidateSelector, tableDataLayoutMetadataList);
    log.info("Selected {} strategies", selectedTableDataLayoutMetadataList.size());
    for (TableDataLayoutMetadata metadata : selectedTableDataLayoutMetadataList) {
      log.info("Selected metadata {}", metadata);
    }
    double totalComputeCost =
        selectedTableDataLayoutMetadataList.stream()
            .map(m -> m.getDataLayoutStrategy().getCost())
            .reduce(0.0, Double::sum);
    double totalReducedFileCount =
        selectedTableDataLayoutMetadataList.stream()
            .map(m -> m.getDataLayoutStrategy().getGain())
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
    return processMetadataList(selectedTableDataLayoutMetadataList, jobType, operationMode);
  }

  private List<OperationTask<?>> processMetadataList(
      List<? extends Metadata> metadataList,
      JobConf.JobTypeEnum jobType,
      OperationMode operationMode) {
    List<OperationTask<?>> taskList = new ArrayList<>();
    for (Metadata metadata : metadataList) {
      log.info("Found metadata {}", metadata);
      Optional<OperationTask<?>> optionalOperationTask =
          processMetadata(metadata, jobType, operationMode);
      if (optionalOperationTask.isPresent()) {
        taskList.add(optionalOperationTask.get());
      }
    }
    return taskList;
  }

  @VisibleForTesting
  protected Optional<OperationTask<?>> processMetadata(
      Metadata metadata, JobConf.JobTypeEnum jobType, OperationMode operationMode) {
    try {
      OperationTask<?> task = taskFactory.create(metadata);
      if (!task.shouldRun()) {
        log.info("Skipping task {}", task);
        return Optional.empty();
      } else {
        if (OperationMode.SUBMIT.equals(operationMode)) {
          task.setJobInfoManager(jobInfoManager);
        }
        task.setOperationMode(operationMode);
        return Optional.of(task);
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Cannot create operation task metadata %s given job type: %s", metadata, jobType),
          e);
    }
  }

  public Optional<OperationTask<?>> createStatusOperationTask(
      JobConf.JobTypeEnum jobType, Metadata metadata, String jobId, OperationMode operationMode) {
    try {
      OperationTask<?> task = taskFactory.create(metadata);
      if (!task.shouldRun()) {
        log.info("Skipping task {}", task);
        return Optional.empty();
      } else {
        if (!OperationMode.POLL.equals(operationMode)) {
          throw new RuntimeException(
              String.format(
                  "Cannot create operation task metadata %s given job type: %s due to invalid operation mode: %s",
                  metadata, jobType));
        }
        task.setJobId(jobId);
        task.setOperationMode(operationMode);
        task.setJobInfoManager(jobInfoManager);
        return Optional.of(task);
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Cannot create operation task metadata %s given job type: %s", metadata, jobType),
          e);
    }
  }

  /**
   * Fetches tables and associated metadata from Tables Service, and builds the operation task list.
   */
  public List<OperationTask<?>> buildOperationTaskList(
      JobConf.JobTypeEnum jobType,
      Properties properties,
      Meter meter,
      OperationMode operationMode) {
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
        return prepareTableOperationTaskList(jobType, operationMode);
      case REPLICATION:
        return prepareReplicationOperationTaskList(jobType, operationMode);
      case DATA_LAYOUT_STRATEGY_EXECUTION:
        return prepareDataLayoutOperationTaskList(jobType, properties, meter, operationMode);
      case ORPHAN_DIRECTORY_DELETION:
        return prepareTableDirectoryOperationTaskList(jobType, operationMode);
      default:
        throw new UnsupportedOperationException(
            String.format("Job type %s is not supported", jobType));
    }
  }

  /**
   * Uses Flux to iterate the databases list in parallel. For each database, list tables
   * asynchronously(using Mono) and then for each table get table metadata asynchronously (using
   * Mono) and add to the queue. On after terminate (when all the parallel threads finishes and were
   * shutdown), the metadata fetch flag is set to true.
   *
   * @param jobType
   * @param operationMode
   */
  public void buildOperationTaskListInParallel(
      JobConf.JobTypeEnum jobType, OperationMode operationMode) {
    List<String> databases = tablesClient.getDatabases();
    Flux.fromIterable(databases) // iterate databases list
        .parallel(
            numParallelMetadataFetch) // Parallelize the databases list processing with N threads
        .runOn(Schedulers.boundedElastic()) // Use boundedElastic scheduler for IO-bound tasks
        .filter(database -> tablesClient.applyDatabaseFilter(database))
        .flatMap(
            database ->
                getAllTables(database)
                    .flatMapMany(Flux::fromIterable)
                    .parallel(numParallelMetadataFetch) // Parallelize the get table metadate with N
                    // threads
                    .runOn(Schedulers.boundedElastic())
                    .flatMap(
                        tableResponseBody ->
                            getTableMetadata(
                                tableResponseBody)) // Get tableMetadata asynchronously for each
            // table
            )
        .doOnNext(
            tableMetadata -> {
              if (tableMetadata != null && tablesClient.applyTableMetadataFilter(tableMetadata)) {
                try {
                  log.debug(
                      "Got table metadata for database name: {}, table name: {} ",
                      tableMetadata.getDbName(),
                      tableMetadata.getTableName());
                  Optional<OperationTask<?>> optionalOperationTask =
                      processMetadata(tableMetadata, jobType, operationMode);
                  if (optionalOperationTask.isPresent()) {
                    // Put tableMetadata into operation task manager
                    operationTaskManager.addData(optionalOperationTask.get());
                  }
                } catch (InterruptedException e) {
                  log.warn("Interrupted while waiting for table metadata to be processed", e);
                }
              }
            })
        .sequential() // wait for all the threads to finish before terminate
        .doAfterTerminate(
            () -> {
              operationTaskManager.updateDataGenerationCompletion();
              log.info(
                  "The metadata fetched count: {} for the job type: {}",
                  operationTaskManager.getTotalDataCount(),
                  jobType);
            })
        .subscribe();
  }

  /**
   * Fetch all tables for the given database asynchronously using Mono.
   *
   * @param database
   * @return Mono<List<GetTableResponseBody>>
   */
  private Mono<List<GetTableResponseBody>> getAllTables(String database) {
    return Mono.fromCallable(
            () -> {
              GetAllTablesResponseBody allTablesResponseBody = tablesClient.getAllTables(database);
              log.debug("Got all tables: {} for database {}", allTablesResponseBody, database);
              if (allTablesResponseBody == null) {
                return Collections.<GetTableResponseBody>emptyList();
              }
              return allTablesResponseBody.getResults();
            })
        .onErrorResume(
            ex -> {
              log.error("Error while fetching tables for database {}", database, ex);
              return Mono.just(Collections.emptyList());
            })
        .subscribeOn(
            Schedulers.boundedElastic()); // Offload the blocking call to boundedElastic scheduler
  }

  /**
   * Fetch table metadata for a table asynchronously using Mono.
   *
   * @param getTableResponseBody
   * @return Mono<TableMetadata>
   */
  private Mono<TableMetadata> getTableMetadata(GetTableResponseBody getTableResponseBody) {
    return Mono.fromCallable(
            () -> tablesClient.mapTableResponseToTableMetadata(getTableResponseBody))
        .flatMap(
            optionalTableMetadata -> {
              if (optionalTableMetadata.isPresent()) {
                log.debug("Got table metadata for : {}", optionalTableMetadata.get());
                return Mono.just(optionalTableMetadata.get());
              } else {
                return Mono.empty();
              }
            })
        .onErrorResume(
            ex -> {
              log.error("Error while fetching table metadata for : {}", getTableResponseBody, ex);
              return Mono.empty();
            })
        .subscribeOn(
            Schedulers.boundedElastic()); // Offload the blocking call to boundedElastic scheduler
  }
}
