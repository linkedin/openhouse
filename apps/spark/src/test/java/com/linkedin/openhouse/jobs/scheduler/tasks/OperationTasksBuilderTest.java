package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.jobs.util.DatabaseMetadata;
import com.linkedin.openhouse.jobs.util.Metadata;
import com.linkedin.openhouse.jobs.util.RetentionConfig;
import com.linkedin.openhouse.jobs.util.TableDataLayoutMetadata;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Mono;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class OperationTasksBuilderTest {
  private static final String METRICS_SCOPE = OperationTasksBuilderTest.class.getName();
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
  @Mock private TablesClient tablesClient;
  @Mock private JobsClient jobsClient;
  private final Properties properties = new Properties();
  private Metadata tableMetadata;
  private List<String> databases = new ArrayList<>();
  private Map<String, GetAllTablesResponseBody> dbAllTables = new HashMap();
  private Map<GetAllTablesResponseBody, List<GetTableResponseBody>> allTablesBodyToGetTableList =
      new HashMap();
  private Map<GetTableResponseBody, Optional<TableMetadata>> getTableResponseToTableMetadata =
      new HashMap();
  private int dbCount = 4;
  private int tableCount = 4;
  List<TableMetadata> tableMetadataList = new ArrayList<>();
  List<TableDataLayoutMetadata> tableDataLayoutMetadataList = new ArrayList<>();
  List<DatabaseMetadata> databaseMetadataList = new ArrayList<>();
  Map<JobConf.JobTypeEnum, Class<? extends OperationTask<?>>> jobTypeToClassMap =
      new HashMap() {
        {
          put(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION, TableSnapshotsExpirationTask.class);
          put(JobConf.JobTypeEnum.RETENTION, TableRetentionTask.class);
          put(JobConf.JobTypeEnum.ORPHAN_FILES_DELETION, TableOrphanFilesDeletionTask.class);
          put(JobConf.JobTypeEnum.TABLE_STATS_COLLECTION, TableStatsCollectionTask.class);
          put(
              JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_GENERATION,
              TableDataLayoutStrategyGenerationTask.class);
          put(
              JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_EXECUTION,
              TableDataLayoutStrategyExecutionTask.class);
          put(JobConf.JobTypeEnum.TABLE_DIRECTORY_DELETION, TableDirectoryDeletionTask.class);
          put(JobConf.JobTypeEnum.DATA_COMPACTION, TableDataCompactionTask.class);
          put(JobConf.JobTypeEnum.STAGED_FILES_DELETION, TableStagedFilesDeletionTask.class);
          put(JobConf.JobTypeEnum.SORT_STATS_COLLECTION, SortStatsCollectionTask.class);
          put(
              JobConf.JobTypeEnum.ORPHAN_FILES_DELETION_BATCH,
              BatchedTableOrphanFilesDeletionTask.class);
        }
      };

  @BeforeAll
  void setup() {
    MockitoAnnotations.openMocks(this);
    tableMetadata =
        TableMetadata.builder().dbName("db1").tableName("test_table").isPrimary(true).build();
    for (int i = 0; i < dbCount; i++) {
      databases.add("db" + i);
    }
    properties.setProperty(OperationTasksBuilder.MAX_STRATEGIES_COUNT, "100");
    populateDbAllTables(dbCount, tableCount);
    prepareMetadata();
  }

  @Test
  public void testCreateStatusOperationTask() {
    OperationTasksBuilder operationTasksBuilder =
        createOperationTasksBuilder(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION);
    Optional<OperationTask<?>> optionalOperationTask =
        operationTasksBuilder.createStatusOperationTask(
            JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION,
            tableMetadata,
            UUID.randomUUID().toString(),
            OperationMode.POLL,
            otelEmitter);
    Assertions.assertNotNull(optionalOperationTask);
    Assertions.assertTrue(optionalOperationTask.isPresent());
    Assertions.assertTrue(optionalOperationTask.get() instanceof OperationTask);
    OperationTask<?> operationTask = optionalOperationTask.get();
    Assertions.assertEquals(OperationMode.POLL, operationTask.operationMode);
    Assertions.assertNotNull(operationTask.jobId);
    Assertions.assertNotNull(operationTask.jobInfoManager);
  }

  @Test
  public void testProcessMetadataSubmitOperation() {
    OperationTasksBuilder operationTasksBuilder =
        createOperationTasksBuilder(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION);
    Optional<OperationTask<?>> optionalOperationTask =
        operationTasksBuilder.processMetadata(
            tableMetadata,
            JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION,
            OperationMode.SUBMIT,
            otelEmitter);
    Assertions.assertNotNull(optionalOperationTask);
    Assertions.assertTrue(optionalOperationTask.isPresent());
    Assertions.assertTrue(optionalOperationTask.get() instanceof TableSnapshotsExpirationTask);
    OperationTask<?> operationTask = optionalOperationTask.get();
    Assertions.assertEquals(OperationMode.SUBMIT, operationTask.operationMode);
    Assertions.assertNull(operationTask.jobId);
    Assertions.assertNotNull(operationTask.jobInfoManager);
  }

  @Test
  public void testBuildOperationTaskListInParallelVerifyAll() throws InterruptedException {
    prepareMockitoForParallelFetch();
    OperationTasksBuilder operationTasksBuilder =
        createOperationTasksBuilder(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION);
    operationTasksBuilder.buildOperationTaskListInParallel(
        JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION, null, otelEmitter, OperationMode.SUBMIT);
    OperationTaskManager operationTaskManager = operationTasksBuilder.getOperationTaskManager();
    // Make sure operation task build is completed
    int count = 16;
    // Make sure operation task build is completed
    do {} while (!operationTaskManager.isDataGenerationCompleted());
    // Poll from the queue until there is no element
    do {
      Assertions.assertEquals(count, operationTaskManager.getCurrentDataCount());
      OperationTask<?> task = operationTaskManager.getData();
      --count;
      // The original table metadata size is 16 and after 16th item, the queue item is returned as
      // null
      if (count == -1) {
        Assertions.assertNull(task);
      } else {
        Assertions.assertNotNull(task);
        Assertions.assertTrue(task instanceof TableSnapshotsExpirationTask);
        Assertions.assertEquals(OperationMode.SUBMIT, task.operationMode);
        Assertions.assertNotNull(task.jobInfoManager);
      }
    } while (operationTaskManager.hasNext());
    Assertions.assertTrue(operationTaskManager.isEmpty());
    Assertions.assertEquals(0, operationTaskManager.getCurrentDataCount());
    Assertions.assertEquals(16, operationTaskManager.getTotalDataCount());
  }

  @Test
  public void testBuildOperationTaskListInParallel() throws InterruptedException {
    List<JobConf.JobTypeEnum> jobList =
        Arrays.asList(
            JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION,
            JobConf.JobTypeEnum.RETENTION,
            JobConf.JobTypeEnum.ORPHAN_FILES_DELETION,
            JobConf.JobTypeEnum.TABLE_STATS_COLLECTION,
            JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_GENERATION,
            JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_EXECUTION,
            JobConf.JobTypeEnum.TABLE_DIRECTORY_DELETION);
    for (JobConf.JobTypeEnum jobType : jobList) {
      prepareMockitoForParallelFetch();
      OperationTasksBuilder operationTasksBuilder = createOperationTasksBuilder(jobType);
      operationTasksBuilder.buildOperationTaskListInParallel(
          jobType, properties, otelEmitter, OperationMode.SUBMIT);
      // Make sure operation task build is completed
      OperationTaskManager operationTaskManager = operationTasksBuilder.getOperationTaskManager();
      do {} while (!operationTaskManager.isDataGenerationCompleted());
      Assertions.assertFalse(operationTaskManager.isEmpty());
      if (jobType == JobConf.JobTypeEnum.TABLE_DIRECTORY_DELETION) {
        Assertions.assertEquals(4, operationTaskManager.getCurrentDataCount());
        Assertions.assertEquals(4, operationTaskManager.getTotalDataCount());
      } else {
        Assertions.assertEquals(16, operationTaskManager.getCurrentDataCount());
        Assertions.assertEquals(16, operationTaskManager.getTotalDataCount());
      }
      OperationTask<?> task = operationTaskManager.getData();
      Assertions.assertNotNull(task);
      Class<? extends OperationTask<?>> taskClass = jobTypeToClassMap.get(jobType);
      Assertions.assertTrue(taskClass.isInstance(task));
      Assertions.assertEquals(OperationMode.SUBMIT, task.operationMode);
      Assertions.assertNotNull(task.jobInfoManager);
    }
  }

  @Test
  public void testBuildOperationTaskList() {
    List<JobConf.JobTypeEnum> jobList =
        Arrays.asList(
            JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION,
            JobConf.JobTypeEnum.RETENTION,
            JobConf.JobTypeEnum.ORPHAN_FILES_DELETION,
            JobConf.JobTypeEnum.TABLE_STATS_COLLECTION,
            JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_GENERATION,
            JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_EXECUTION,
            JobConf.JobTypeEnum.TABLE_DIRECTORY_DELETION);
    for (JobConf.JobTypeEnum jobType : jobList) {
      if (jobType == JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_EXECUTION) {
        Mockito.when(tablesClient.getTableDataLayoutMetadataList())
            .thenReturn(tableDataLayoutMetadataList);
      } else if (jobType == JobConf.JobTypeEnum.TABLE_DIRECTORY_DELETION) {
        Mockito.when(tablesClient.getDatabaseMetadataList()).thenReturn(databaseMetadataList);
      } else {
        Mockito.when(tablesClient.getTableMetadataList()).thenReturn(tableMetadataList);
      }
      OperationTasksBuilder operationTasksBuilder = createOperationTasksBuilder(jobType);
      List<OperationTask<?>> operationTasks =
          operationTasksBuilder.buildOperationTaskList(
              jobType, properties, otelEmitter, OperationMode.SINGLE);
      Assertions.assertNotNull(operationTasks);
      if (jobType == JobConf.JobTypeEnum.TABLE_DIRECTORY_DELETION) {
        Assertions.assertEquals(4, operationTasks.size());
        for (int i = 0; i < 4; i++) {
          Assertions.assertNotNull(operationTasks.get(i));
          Class<? extends OperationTask<?>> taskClass = jobTypeToClassMap.get(jobType);
          Assertions.assertTrue(taskClass.isInstance(operationTasks.get(i)));
          Assertions.assertEquals(OperationMode.SINGLE, operationTasks.get(i).operationMode);
        }
        continue;
      } else {
        // Make sure operation task build is completed
        Assertions.assertEquals(16, operationTasks.size());
        for (int i = 0; i < 16; i++) {
          Assertions.assertNotNull(operationTasks.get(i));
          Class<? extends OperationTask<?>> taskClass = jobTypeToClassMap.get(jobType);
          Assertions.assertTrue(taskClass.isInstance(operationTasks.get(i)));
          Assertions.assertEquals(OperationMode.SINGLE, operationTasks.get(i).operationMode);
        }
      }
    }
  }

  private OperationTasksBuilder createOperationTasksBuilder(JobConf.JobTypeEnum jobType) {
    OperationTaskFactory<? extends OperationTask<?>> tasksFactory =
        new OperationTaskFactory<>(
            jobTypeToClassMap.get(jobType), jobsClient, tablesClient, 60000L, 60000L, 120000L);
    return new OperationTasksBuilder(
        tasksFactory,
        tablesClient,
        2,
        new OperationTaskManager(jobType),
        new JobInfoManager(jobType));
  }

  @ParameterizedTest
  @EnumSource(
      value = JobConf.JobTypeEnum.class,
      names = {
        "DATA_COMPACTION",
        "DATA_LAYOUT_STRATEGY_GENERATION",
        "DATA_LAYOUT_STRATEGY_EXECUTION",
        "RETENTION",
        "SNAPSHOTS_EXPIRATION",
        "ORPHAN_FILES_DELETION",
        "STAGED_FILES_DELETION"
      })
  void testTemporaryMaintenancePlanningAndOrdinaryControls(JobConf.JobTypeEnum jobType) {
    OperationTasksBuilder builder = createOperationTasksBuilder(jobType);
    TableMetadata.TableMetadataBuilder<?, ?> metadata =
        jobType == JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_EXECUTION
            ? TableDataLayoutMetadata.builder()
                .dataLayoutStrategy(DataLayoutStrategy.builder().gain(1).build())
            : TableMetadata.builder();
    metadata
        .dbName("db")
        .tableName("maintenance_control")
        .isPrimary(true)
        .isClustered(true)
        .retentionConfig(RetentionConfig.builder().columnName("event_time").build());

    Assertions.assertTrue(
        builder
            .processMetadata(metadata.build(), jobType, OperationMode.SUBMIT, otelEmitter)
            .isPresent());
    Assertions.assertFalse(
        builder
            .processMetadata(
                metadata.isTemporary(true).build(), jobType, OperationMode.SUBMIT, otelEmitter)
            .isPresent());
    Assertions.assertTrue(
        builder
            .processMetadata(
                metadata.isTemporary(false).build(), jobType, OperationMode.SUBMIT, otelEmitter)
            .isPresent());
    Assertions.assertFalse(
        builder
            .processMetadata(
                metadata
                    .jobExecutionProperties(Collections.singletonMap("disabled", "true"))
                    .build(),
                jobType,
                OperationMode.SUBMIT,
                otelEmitter)
            .isPresent());
    boolean runsOnReplica =
        jobType == JobConf.JobTypeEnum.ORPHAN_FILES_DELETION
            || jobType == JobConf.JobTypeEnum.STAGED_FILES_DELETION;
    Assertions.assertEquals(
        runsOnReplica,
        builder
            .processMetadata(
                metadata.jobExecutionProperties(Collections.emptyMap()).isPrimary(false).build(),
                jobType,
                OperationMode.SUBMIT,
                otelEmitter)
            .isPresent());
  }

  @ParameterizedTest
  @EnumSource(
      value = JobConf.JobTypeEnum.class,
      names = {"TABLE_STATS_COLLECTION", "SORT_STATS_COLLECTION"})
  void testTemporaryStatisticsStillRespectExplicitDisable(JobConf.JobTypeEnum jobType) {
    OperationTasksBuilder builder = createOperationTasksBuilder(jobType);
    TableMetadata.TableMetadataBuilder<?, ?> metadata =
        TableMetadata.builder()
            .dbName("db")
            .tableName("temporary")
            .isPrimary(true)
            .isTemporary(true);
    Assertions.assertTrue(
        builder
            .processMetadata(metadata.build(), jobType, OperationMode.SUBMIT, otelEmitter)
            .isPresent());
    Assertions.assertFalse(
        builder
            .processMetadata(
                metadata
                    .jobExecutionProperties(Collections.singletonMap(jobType + ".disabled", "true"))
                    .build(),
                jobType,
                OperationMode.SUBMIT,
                otelEmitter)
            .isPresent());
  }

  @Test
  void testTemporaryTablesAreExcludedFromOrphanFileBatches() {
    TableMetadata temporary =
        TableMetadata.builder().dbName("db").tableName("temporary").isTemporary(true).build();
    TableMetadata ordinary = TableMetadata.builder().dbName("db").tableName("ordinary").build();
    Mockito.when(tablesClient.getTableMetadataList())
        .thenReturn(Arrays.asList(temporary, ordinary));
    List<OperationTask<?>> tasks =
        createOperationTasksBuilder(JobConf.JobTypeEnum.ORPHAN_FILES_DELETION_BATCH)
            .buildOperationTaskList(
                JobConf.JobTypeEnum.ORPHAN_FILES_DELETION_BATCH,
                properties,
                otelEmitter,
                OperationMode.SUBMIT);
    Assertions.assertEquals(1, tasks.size());
    Assertions.assertEquals(Arrays.asList("--tableNames", "db.ordinary"), tasks.get(0).getArgs());
  }

  @ParameterizedTest
  @CsvSource({"false,1,100", "true,1,100", "false,10,1", "true,10,1"})
  void testTemporaryStrategiesDoNotConsumeLayoutBudget(
      boolean parallel, int maxStrategiesCount, int maxComputeCost) throws InterruptedException {
    JobConf.JobTypeEnum jobType = JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_EXECUTION;
    TableDataLayoutMetadata.TableDataLayoutMetadataBuilder<?, ?> metadata =
        TableDataLayoutMetadata.builder().dbName("db").isPrimary(true);
    TableDataLayoutMetadata temporary =
        metadata
            .tableName("temporary")
            .isTemporary(true)
            .dataLayoutStrategy(DataLayoutStrategy.builder().gain(100).cost(1).build())
            .build();
    TableDataLayoutMetadata ordinary =
        metadata
            .tableName("ordinary")
            .isTemporary(false)
            .dataLayoutStrategy(DataLayoutStrategy.builder().gain(10).cost(1).build())
            .build();
    TablesClient planningClient = Mockito.mock(TablesClient.class);
    Mockito.when(planningClient.getTableDataLayoutMetadataList())
        .thenReturn(Arrays.asList(temporary, ordinary));
    Mockito.when(planningClient.getTableDataLayoutMetadataListInParallel(2))
        .thenReturn(Arrays.asList(temporary, ordinary));
    Properties selectionProperties = new Properties();
    selectionProperties.setProperty(
        OperationTasksBuilder.MAX_STRATEGIES_COUNT, Integer.toString(maxStrategiesCount));
    selectionProperties.setProperty(
        OperationTasksBuilder.MAX_COST_BUDGET_GB_HRS, Integer.toString(maxComputeCost));
    OtelEmitter planningEmitter = Mockito.mock(OtelEmitter.class);
    OperationTaskManager manager = new OperationTaskManager(jobType);
    OperationTasksBuilder builder =
        new OperationTasksBuilder(
            new OperationTaskFactory<>(
                TableDataLayoutStrategyExecutionTask.class,
                jobsClient,
                planningClient,
                60000L,
                60000L,
                120000L),
            planningClient,
            2,
            manager,
            new JobInfoManager(jobType));
    OperationTask<?> selectedTask;
    if (parallel) {
      builder.buildOperationTaskListInParallel(
          jobType, selectionProperties, planningEmitter, OperationMode.SUBMIT);
      Assertions.assertTimeoutPreemptively(
          Duration.ofSeconds(5),
          () -> {
            while (!manager.isDataGenerationCompleted()) {
              Thread.sleep(10);
            }
          });
      Assertions.assertEquals(1, manager.getTotalDataCount());
      selectedTask = manager.getData();
    } else {
      List<OperationTask<?>> tasks =
          builder.buildOperationTaskList(
              jobType, selectionProperties, planningEmitter, OperationMode.SUBMIT);
      Assertions.assertEquals(1, tasks.size());
      selectedTask = tasks.get(0);
    }
    Assertions.assertEquals("db.ordinary", selectedTask.metadata.getEntityName());
    Mockito.verify(planningEmitter)
        .count(
            Mockito.anyString(),
            Mockito.eq("data_layout_optimization_estimated_compute_cost"),
            Mockito.eq(1L),
            Mockito.isNull());
    Mockito.verify(planningEmitter)
        .count(
            Mockito.anyString(),
            Mockito.eq("data_layout_optimization_estimated_reduced_file_count"),
            Mockito.eq(10L),
            Mockito.isNull());
  }

  private void populateDbAllTables(int dbCount, int tableCount) {
    for (int i = 0; i < dbCount; i++) {
      GetAllTablesResponseBody allTablesResponseBody = Mockito.mock(GetAllTablesResponseBody.class);
      dbAllTables.put("db" + i, allTablesResponseBody);
      List<GetTableResponseBody> tableResponseBodyList = new ArrayList<>();
      for (int j = 0; j < tableCount; j++) {
        GetTableResponseBody getTableResponseBody = Mockito.mock(GetTableResponseBody.class);
        getTableResponseToTableMetadata.put(getTableResponseBody, getTableMetadata(i, j));
        tableResponseBodyList.add(getTableResponseBody);
      }
      allTablesBodyToGetTableList.put(allTablesResponseBody, tableResponseBodyList);
    }
  }

  private Optional<TableMetadata> getTableMetadata(int dbIndex, int tableIndex) {
    return Optional.of(
        TableMetadata.builder()
            .dbName("db" + dbIndex)
            .tableName("test_table" + tableIndex)
            .isPrimary(true)
            .retentionConfig(RetentionConfig.builder().columnName("column").build())
            .build());
  }

  private List<TableDataLayoutMetadata> getTableDataLayoutMetadata(int dbIndex, int tableIndex) {
    return Arrays.asList(
        TableDataLayoutMetadata.builder()
            .dbName("db" + dbIndex)
            .tableName("test_table" + tableIndex)
            .isPrimary(true)
            .dataLayoutStrategy(
                DataLayoutStrategy.builder().gain(dbIndex + tableIndex * 0.1 + 1).build())
            .build());
  }

  private Optional<DatabaseMetadata> getDatabaseMetadata(int dbIndex) {
    return Optional.of(DatabaseMetadata.builder().dbName("db" + dbIndex).build());
  }

  private void prepareMetadata() {
    for (int i = 0; i < dbCount; i++) {
      for (int j = 0; j < tableCount; j++) {
        tableMetadataList.add(getTableMetadata(i, j).get());
        tableDataLayoutMetadataList.addAll(getTableDataLayoutMetadata(i, j));
      }
      databaseMetadataList.add(getDatabaseMetadata(i).get());
    }
  }

  private void prepareMockitoForParallelFetch() {
    Mockito.when(tablesClient.getDatabases()).thenReturn(databases);
    Mockito.when(tablesClient.applyDatabaseFilter(Mockito.anyString())).thenReturn(true);
    for (int i = 0; i < dbCount; i++) {
      GetAllTablesResponseBody getAllTablesResponseBody = dbAllTables.get("db" + i);
      Mockito.when(tablesClient.getAllTables("db" + i)).thenReturn(getAllTablesResponseBody);
      List<GetTableResponseBody> getTableResponseBodyList =
          allTablesBodyToGetTableList.get(getAllTablesResponseBody);
      Mockito.when(getAllTablesResponseBody.getResults()).thenReturn(getTableResponseBodyList);
      Mockito.when(tablesClient.getAllTablesAsync("db" + i))
          .thenReturn(Mono.just(getTableResponseBodyList));
      for (int j = 0; j < tableCount; j++) {
        GetTableResponseBody getTableResponseBody = getTableResponseBodyList.get(j);
        Mockito.when(tablesClient.getTableMetadataAsync(getTableResponseBody))
            .thenReturn(Mono.just(getTableResponseToTableMetadata.get(getTableResponseBody).get()));
        Mockito.when(tablesClient.getTableDataLayoutMetadataListInParallel(Mockito.anyInt()))
            .thenReturn(tableDataLayoutMetadataList);
      }
    }
    Mockito.when(tablesClient.getDatabaseMetadataList()).thenReturn(databaseMetadataList);
    Mockito.when(tablesClient.applyDatabaseFilter(Mockito.anyString())).thenReturn(true);
    Mockito.when(tablesClient.applyTableMetadataFilter(Mockito.any(TableMetadata.class)))
        .thenReturn(true);
  }
}
