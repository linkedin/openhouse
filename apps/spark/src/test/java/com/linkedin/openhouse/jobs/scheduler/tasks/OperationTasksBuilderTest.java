package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.common.OtelEmitter;
import com.linkedin.openhouse.datalayout.strategy.DataLayoutStrategy;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.jobs.util.Metadata;
import com.linkedin.openhouse.jobs.util.RetentionConfig;
import com.linkedin.openhouse.jobs.util.TableDataLayoutMetadata;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Mono;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class OperationTasksBuilderTest {
  private static final String METRICS_SCOPE = OperationTasksBuilderTest.class.getName();
  private final OtelEmitter otelEmitter = AppsOtelEmitter.getInstance();
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
            OperationMode.POLL);
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
            JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_EXECUTION);
    for (JobConf.JobTypeEnum jobType : jobList) {
      prepareMockitoForParallelFetch();
      OperationTasksBuilder operationTasksBuilder = createOperationTasksBuilder(jobType);
      operationTasksBuilder.buildOperationTaskListInParallel(
          jobType, properties, otelEmitter, OperationMode.SUBMIT);
      // Make sure operation task build is completed
      OperationTaskManager operationTaskManager = operationTasksBuilder.getOperationTaskManager();
      do {} while (!operationTaskManager.isDataGenerationCompleted());
      Assertions.assertFalse(operationTaskManager.isEmpty());
      Assertions.assertEquals(16, operationTaskManager.getCurrentDataCount());
      Assertions.assertEquals(16, operationTaskManager.getTotalDataCount());
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
            JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_EXECUTION);
    for (JobConf.JobTypeEnum jobType : jobList) {
      if (jobType == JobConf.JobTypeEnum.DATA_LAYOUT_STRATEGY_EXECUTION) {
        Mockito.when(tablesClient.getTableDataLayoutMetadataList())
            .thenReturn(tableDataLayoutMetadataList);
      } else {
        Mockito.when(tablesClient.getTableMetadataList()).thenReturn(tableMetadataList);
      }
      OperationTasksBuilder operationTasksBuilder = createOperationTasksBuilder(jobType);
      List<OperationTask<?>> operationTasks =
          operationTasksBuilder.buildOperationTaskList(
              jobType, properties, otelEmitter, OperationMode.SINGLE);
      Assertions.assertNotNull(operationTasks);
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

  private void prepareMetadata() {
    for (int i = 0; i < dbCount; i++) {
      for (int j = 0; j < tableCount; j++) {
        tableMetadataList.add(getTableMetadata(i, j).get());
        tableDataLayoutMetadataList.addAll(getTableDataLayoutMetadata(i, j));
      }
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
    Mockito.when(tablesClient.applyDatabaseFilter(Mockito.anyString())).thenReturn(true);
    Mockito.when(tablesClient.applyTableMetadataFilter(Mockito.any(TableMetadata.class)))
        .thenReturn(true);
  }
}
