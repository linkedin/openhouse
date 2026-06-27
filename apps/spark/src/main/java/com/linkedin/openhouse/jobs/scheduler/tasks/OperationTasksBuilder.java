package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.datalayout.ranker.DataLayoutCandidateSelector;
import com.linkedin.openhouse.datalayout.ranker.DataLayoutStrategyScorer;
import com.linkedin.openhouse.datalayout.ranker.GreedyMaxBudgetCandidateSelector;
import com.linkedin.openhouse.datalayout.ranker.SimpleWeightedSumDataLayoutStrategyScorer;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.scheduler.JobsScheduler;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.DataLayoutUtil;
import com.linkedin.openhouse.jobs.util.DatabaseMetadata;
import com.linkedin.openhouse.jobs.util.DirectoryMetadata;
import com.linkedin.openhouse.jobs.util.Metadata;
import com.linkedin.openhouse.jobs.util.TableDataLayoutMetadata;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.jobs.util.TableMetadataBatch;
import com.linkedin.openhouse.optimizer.binpack.BinItem;
import com.linkedin.openhouse.optimizer.binpack.FirstFitDecreasingBinPacker;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
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
  public static final String BATCH_MAX_ITEMS = "batchMaxItems";
  private static final double COMPUTE_COST_WEIGHT_DEFAULT = 0.3;
  private static final double COMPACTION_GAIN_WEIGHT_DEFAULT = 0.7;
  private static final double MAX_COST_BUDGET_GB_HRS_DEFAULT = 1000.0;
  private static final int MAX_STRATEGIES_COUNT_DEFAULT = 10;
  /**
   * Default value for the {@code --batchMaxItems} CLI option (operator-tunable). Not a hard limit:
   * the Spark-app side enforces the actual ceiling via {@link AppConstants#OFD_MAX_BATCH_SIZE}, and
   * {@link JobsScheduler#getAdditionalProperties} rejects any operator-supplied value above it.
   */
  private static final int DEFAULT_BATCH_MAX_ITEMS = 25;

  private static final String METRICS_SCOPE = JobsScheduler.class.getName();

  private final OperationTaskFactory<? extends OperationTask<?>> taskFactory;

  protected final TablesClient tablesClient;

  private final int numParallelMetadataFetch;

  @Getter(AccessLevel.PROTECTED)
  private final OperationTaskManager operationTaskManager;

  @Getter(AccessLevel.PROTECTED)
  private final JobInfoManager jobInfoManager;

  private List<OperationTask<?>> prepareTableOperationTaskList(
      JobConf.JobTypeEnum jobType, OperationMode operationMode, OtelEmitter otelEmitter) {
    List<TableMetadata> tableMetadataList = tablesClient.getTableMetadataList();
    log.info("Fetched metadata for {} tables", tableMetadataList.size());
    return processMetadataList(tableMetadataList, jobType, operationMode, otelEmitter);
  }

  /**
   * Builds one {@link BatchedTableOrphanFilesDeletionTask} per database-scoped bin. Groups eligible
   * tables by database (batches never cross databases), then applies the first-fit-decreasing bin
   * packer with a per-bin item cap from {@code properties} (defaults to {@value
   * #DEFAULT_BATCH_MAX_ITEMS}). Tables with the maintenance op disabled are filtered out before
   * grouping.
   */
  private List<OperationTask<?>> prepareBatchedOrphanFilesDeletionTaskList(
      JobConf.JobTypeEnum jobType,
      Properties properties,
      OperationMode operationMode,
      OtelEmitter otelEmitter) {
    // Bounds (maxItemsPerBin <= AppConstants.OFD_MAX_BATCH_SIZE) are enforced at the CLI parsing
    // boundary in JobsScheduler.getAdditionalProperties; this method assumes validated input.
    int maxItemsPerBin =
        NumberUtils.toInt(properties.getProperty(BATCH_MAX_ITEMS), DEFAULT_BATCH_MAX_ITEMS);
    // Item-count cap only; the libs default maxWeightPerBin (1_000_000) is effectively unbounded
    // for weight=1 items, so item-count is the active constraint until table_stats is wired in.
    FirstFitDecreasingBinPacker<BinItem> packer =
        FirstFitDecreasingBinPacker.<BinItem>builder().maxItemsPerBin(maxItemsPerBin).build();

    // Note: groupingBy materializes the full eligible-table list in driver memory and serializes
    // the pipeline at that point — acceptable for the O(thousands) tables we expect, but worth
    // revisiting if the bound grows.
    List<TableMetadataBatch> batches =
        tablesClient.getTableMetadataList().stream()
            .filter(t -> !t.isMaintenanceJobDisabled(jobType))
            .collect(Collectors.groupingBy(TableMetadata::getDbName))
            .entrySet()
            .stream()
            .flatMap(
                dbGroup ->
                    packer
                        .pack(
                            dbGroup.getValue().stream()
                                .map(TableMetadataBinItem::new)
                                .collect(Collectors.toList()))
                        .stream()
                        .map(
                            group ->
                                TableMetadataBatch.builder()
                                    .dbName(dbGroup.getKey())
                                    .tables(
                                        group.stream()
                                            .map(TableMetadataBinItem::getMetadata)
                                            .collect(Collectors.toList()))
                                    .build()))
            .collect(Collectors.toList());
    log.info(
        "Packed eligible tables into {} batches; binMaxItems={}", batches.size(), maxItemsPerBin);
    return processMetadataList(batches, jobType, operationMode, otelEmitter);
  }

  private List<OperationTask<?>> prepareReplicationOperationTaskList(
      JobConf.JobTypeEnum jobType, OperationMode operationMode, OtelEmitter otelEmitter) {
    List<TableMetadata> replicationSetupTableMetadataList = tablesClient.getTableMetadataList();
    // filters tables which are primary and hava replication config defined
    replicationSetupTableMetadataList =
        replicationSetupTableMetadataList.stream()
            .filter(m -> m.isPrimary() && (m.getReplicationConfig() != null))
            .collect(Collectors.toList());
    log.info(
        "Fetched metadata for {} tables for replication setup task",
        replicationSetupTableMetadataList.size());
    return processMetadataList(
        replicationSetupTableMetadataList, jobType, operationMode, otelEmitter);
  }

  private List<OperationTask<?>> prepareTableDirectoryOperationTaskList(
      JobConf.JobTypeEnum jobType, OperationMode operationMode, OtelEmitter otelEmitter) {
    List<DirectoryMetadata> directoryMetadataList = tablesClient.getOrphanTableDirectories();
    log.info("Fetched metadata for {} directories", directoryMetadataList.size());
    return processMetadataList(directoryMetadataList, jobType, operationMode, otelEmitter);
  }

  private List<OperationTask<?>> prepareDataLayoutOperationTaskList(
      JobConf.JobTypeEnum jobType,
      Properties properties,
      OperationMode operationMode,
      OtelEmitter otelEmitter) {
    List<TableDataLayoutMetadata> tableDataLayoutMetadataList =
        tablesClient.getTableDataLayoutMetadataList();
    List<TableDataLayoutMetadata> selectedTableDataLayoutMetadataList =
        rankAndSelectFromTableDataLayoutMetadataList(
            tableDataLayoutMetadataList, properties, otelEmitter);
    return processMetadataList(
        selectedTableDataLayoutMetadataList, jobType, operationMode, otelEmitter);
  }

  private List<TableDataLayoutMetadata> rankAndSelectFromTableDataLayoutMetadataList(
      List<TableDataLayoutMetadata> tableDataLayoutMetadataList,
      Properties properties,
      OtelEmitter otelEmitter) {
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
    otelEmitter.count(
        METRICS_SCOPE,
        "data_layout_optimization_estimated_compute_cost",
        (long) totalComputeCost,
        null);
    otelEmitter.count(
        METRICS_SCOPE,
        "data_layout_optimization_estimated_reduced_file_count",
        (long) totalReducedFileCount,
        null);
    return selectedTableDataLayoutMetadataList;
  }

  private List<OperationTask<?>> prepareDatabaseOperationTaskList(
      JobConf.JobTypeEnum jobType, OperationMode operationMode, OtelEmitter otelEmitter) {
    List<DatabaseMetadata> databaseList = tablesClient.getDatabaseMetadataList();
    log.info("Fetched metadata for {} databases", databaseList.size());
    return processMetadataList(databaseList, jobType, operationMode, otelEmitter);
  }

  private List<OperationTask<?>> processMetadataList(
      List<? extends Metadata> metadataList,
      JobConf.JobTypeEnum jobType,
      OperationMode operationMode,
      OtelEmitter otelEmitter) {
    List<OperationTask<?>> taskList = new ArrayList<>();
    for (Metadata metadata : metadataList) {
      log.info("Found metadata {}", metadata);
      Optional<OperationTask<?>> optionalOperationTask =
          processMetadata(metadata, jobType, operationMode, otelEmitter);
      if (optionalOperationTask.isPresent()) {
        taskList.add(optionalOperationTask.get());
      }
    }
    return taskList;
  }

  @VisibleForTesting
  public Optional<OperationTask<?>> processMetadata(
      Metadata metadata,
      JobConf.JobTypeEnum jobType,
      OperationMode operationMode,
      OtelEmitter otelEmitter) {
    try {
      OperationTask<?> task = taskFactory.create(metadata);
      task.setOtelEmitter(otelEmitter);

      // Publish entity metrics for triggered tasks
      Attributes taskAttributes =
          Attributes.of(
              AttributeKey.stringKey(AppConstants.ENTITY_NAME),
              metadata.getEntityName(),
              AttributeKey.stringKey(AppConstants.ENTITY_TYPE),
              metadata.getClass().getSimpleName().replace("Metadata", ""),
              AttributeKey.stringKey(AppConstants.JOB_TYPE),
              jobType.getValue());
      otelEmitter.count(METRICS_SCOPE, "maintenance_job_triggered", 1, taskAttributes);

      if (!task.shouldRun()) {
        log.info("Skipping task {}", task);

        // Publish entity metrics for skipped tasks
        otelEmitter.count(METRICS_SCOPE, "maintenance_job_skipped", 1, taskAttributes);
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
      JobConf.JobTypeEnum jobType,
      Metadata metadata,
      String jobId,
      OperationMode operationMode,
      OtelEmitter otelEmitter) {
    try {
      OperationTask<?> task = taskFactory.create(metadata);
      task.setOtelEmitter(otelEmitter);
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
      OtelEmitter otelEmitter,
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
      case SORT_STATS_COLLECTION:
        return prepareTableOperationTaskList(jobType, operationMode, otelEmitter);
      case ORPHAN_FILES_DELETION_BATCH:
        return prepareBatchedOrphanFilesDeletionTaskList(
            jobType, properties, operationMode, otelEmitter);
      case REPLICATION:
        return prepareReplicationOperationTaskList(jobType, operationMode, otelEmitter);
      case DATA_LAYOUT_STRATEGY_EXECUTION:
        return prepareDataLayoutOperationTaskList(jobType, properties, operationMode, otelEmitter);
      case ORPHAN_DIRECTORY_DELETION:
        return prepareTableDirectoryOperationTaskList(jobType, operationMode, otelEmitter);
      case TABLE_DIRECTORY_DELETION:
        // Apply operation at a database level to capture bulk directories
        return prepareDatabaseOperationTaskList(jobType, operationMode, otelEmitter);
      default:
        throw new UnsupportedOperationException(
            String.format("Job type %s is not supported", jobType));
    }
  }

  /**
   * Fetches table metadata, builds the operation tasks, and add them to the task queue in parallel.
   */
  public void buildOperationTaskListInParallel(
      JobConf.JobTypeEnum jobType,
      Properties properties,
      OtelEmitter otelEmitter,
      OperationMode operationMode) {
    if (jobType == JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_EXECUTION) {
      // DLO execution job needs to fetch all table metadata before submission
      buildDataLayoutOperationTaskListInParallel(jobType, properties, operationMode, otelEmitter);
    } else if (jobType == JobConf.JobTypeEnum.TABLE_DIRECTORY_DELETION) {
      buildDatabaseLevelOperationTasksInParallel(jobType, operationMode, otelEmitter);
    } else if (jobType == JobConf.JobTypeEnum.ORPHAN_FILES_DELETION_BATCH) {
      // Batched OFD needs the full table set in hand before it can group-by-db and bin-pack,
      // so we use the synchronous fetch path then enqueue the tasks in bulk.
      List<OperationTask<?>> tasks =
          prepareBatchedOrphanFilesDeletionTaskList(
              jobType, properties, operationMode, otelEmitter);
      for (OperationTask<?> task : tasks) {
        try {
          operationTaskManager.addData(task);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.warn("Interrupted while enqueueing batched OFD task", e);
        }
      }
      operationTaskManager.updateDataGenerationCompletion();
      log.info("Enqueued {} batched OFD tasks for job type: {}", tasks.size(), jobType);
    } else {
      buildOperationTaskListInParallelInternal(jobType, operationMode, otelEmitter);
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
  private void buildOperationTaskListInParallelInternal(
      JobConf.JobTypeEnum jobType, OperationMode operationMode, OtelEmitter otelEmitter) {
    List<String> databases = tablesClient.getDatabases();
    Flux.fromIterable(databases) // iterate databases list
        .parallel(
            numParallelMetadataFetch) // Parallelize the databases list processing with N threads
        .runOn(Schedulers.boundedElastic()) // Use boundedElastic scheduler for IO-bound tasks
        .filter(tablesClient::applyDatabaseFilter)
        .flatMap(
            database ->
                tablesClient
                    .getAllTablesAsync(database)
                    .flatMapMany(Flux::fromIterable)
                    .parallel(numParallelMetadataFetch) // Parallelize the get table metadate with N
                    // threads
                    .runOn(Schedulers.boundedElastic())
                    .flatMap(tablesClient::getTableMetadataAsync)
            // Get tableMetadata asynchronously for each table
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
                      processMetadata(tableMetadata, jobType, operationMode, otelEmitter);
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
   * Fetches table data layout metadata for all tables in parallel first, then do rank and select.
   * Then create a task for each metadata in the list, and set the flag to true on terminate.
   */
  private void buildDataLayoutOperationTaskListInParallel(
      JobConf.JobTypeEnum jobType,
      Properties properties,
      OperationMode operationMode,
      OtelEmitter otelEmitter) {
    List<TableDataLayoutMetadata> tableDataLayoutMetadataList =
        tablesClient.getTableDataLayoutMetadataListInParallel(numParallelMetadataFetch);
    List<TableDataLayoutMetadata> selectedTableDataLayoutMetadataList =
        rankAndSelectFromTableDataLayoutMetadataList(
            tableDataLayoutMetadataList, properties, otelEmitter);
    Flux.fromIterable(selectedTableDataLayoutMetadataList)
        .parallel(numParallelMetadataFetch)
        .runOn(Schedulers.boundedElastic())
        .doOnNext(
            tableDataLayoutMetadata -> {
              try {
                log.debug(
                    "Got table data layout metadata {} ",
                    tableDataLayoutMetadata.getDataLayoutStrategy());
                Optional<OperationTask<?>> optionalOperationTask =
                    processMetadata(tableDataLayoutMetadata, jobType, operationMode, otelEmitter);
                if (optionalOperationTask.isPresent()) {
                  operationTaskManager.addData(optionalOperationTask.get());
                }
              } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for table metadata to be processed", e);
              }
            })
        .sequential()
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

  private void buildDatabaseLevelOperationTasksInParallel(
      JobConf.JobTypeEnum jobType, OperationMode operationMode, OtelEmitter otelEmitter) {
    List<DatabaseMetadata> databaseMetadataList = tablesClient.getDatabaseMetadataList();

    Flux.fromIterable(databaseMetadataList)
        .parallel(numParallelMetadataFetch)
        .runOn(Schedulers.boundedElastic())
        .doOnNext(
            databaseMetadata -> {
              try {
                Optional<OperationTask<?>> optionalOperationTask =
                    processMetadata(databaseMetadata, jobType, operationMode, otelEmitter);
                if (optionalOperationTask.isPresent()) {
                  operationTaskManager.addData(optionalOperationTask.get());
                }
              } catch (InterruptedException e) {
                log.warn("Interrupted while processing database for table directory cleanup", e);
              }
            })
        .sequential()
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
   * Minimal {@link BinItem} for the legacy scheduler path. Carries the full {@link TableMetadata}
   * so the caller can reconstruct each bin's table list directly from the packer's output without a
   * fqtn → metadata lookup. Every item has weight 1, so packing is driven purely by {@code
   * maxItemsPerBin}. {@code fromOpAndStats} is unreachable here because we call {@code
   * packer.pack(List<BinItem>)} (the pre-projected overload), not the projection path.
   */
  private static final class TableMetadataBinItem implements BinItem {
    @Getter private final TableMetadata metadata;

    TableMetadataBinItem(TableMetadata metadata) {
      this.metadata = metadata;
    }

    @Override
    public long getWeight() {
      return 1L;
    }

    @Override
    public String getFullyQualifiedTableName() {
      return metadata.fqtn();
    }

    /**
     * Legacy scheduler has no optimizer-side operation ID. The libs {@link BinItem} interface
     * requires {@code String} (not {@code Optional}) so we return empty to satisfy the contract.
     */
    @Override
    public String getOperationId() {
      return "";
    }

    @Override
    public BinItem fromOpAndStats(TableOperationDto op, TableStatsDto stats) {
      return this;
    }
  }
}
