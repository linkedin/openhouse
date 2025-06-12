package com.linkedin.openhouse.jobs.scheduler.tasks;

import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.JobsClientFactory;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.TablesClientFactory;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.util.Metadata;
import com.linkedin.openhouse.jobs.util.RetentionConfig;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class OperationTasksBuilderTest {
  private TablesClient tablesClient;
  private JobsClient jobsClient;
  private Metadata tableMetadata;
  private OperationTasksBuilder operationTasksBuilderSnapshotExpiration;
  private OperationTasksBuilder operationTasksBuilderRetention;
  private OperationTasksBuilder operationTasksBuilderOrphanFileDeletion;
  private OperationTasksBuilder operationTasksBuilderStatsCollection;
  private OperationTaskFactory<TableSnapshotsExpirationTask> tasksFactorySnapshotExpiration;
  private OperationTaskFactory<TableRetentionTask> tasksFactoryRetention;
  private OperationTaskFactory<TableStatsCollectionTask> tasksFactoryStatsCollection;
  private OperationTaskFactory<TableOrphanFilesDeletionTask> tasksFactoryOrphanFileDeletion;
  private OperationTaskManager operationTaskManagerSnapshotExpiration;
  private OperationTaskManager operationTaskManagerOrphanFileDeletion;
  private OperationTaskManager operationTaskManagerStatsCollection;
  private OperationTaskManager operationTaskManagerRetention;
  private JobInfoManager jobInfoManagerSnapshotExpiration;
  private JobInfoManager jobInfoManagerOrphanFileDeletion;
  private JobInfoManager jobInfoManagerStatsCollection;
  private JobInfoManager jobInfoManagerRetention;
  private List<String> databases = new ArrayList<>();
  private Map<String, GetAllTablesResponseBody> dbAllTables = new HashMap();
  private Map<GetAllTablesResponseBody, List<GetTableResponseBody>> allTablesBodyToGetTableList =
      new HashMap();
  private Map<GetTableResponseBody, Optional<TableMetadata>> getTableResponseToTableMetadata =
      new HashMap();
  private int dbCount = 4;
  private int tableCount = 4;
  List<TableMetadata> tableMetadataList = new ArrayList<>();

  @BeforeAll
  void setup() {
    tablesClient = Mockito.mock(TablesClient.class);
    jobsClient = Mockito.mock(JobsClient.class);
    tableMetadata =
        TableMetadata.builder().dbName("db1").tableName("test_table").isPrimary(true).build();
    Class<TableSnapshotsExpirationTask> operationTaskClsSnapshotExpiration =
        TableSnapshotsExpirationTask.class;
    Class<TableRetentionTask> operationTaskClsRetention = TableRetentionTask.class;
    Class<TableStatsCollectionTask> operationTaskClsStatsCollection =
        TableStatsCollectionTask.class;
    Class<TableOrphanFilesDeletionTask> operationTaskClsOrphanFileDeletion =
        TableOrphanFilesDeletionTask.class;
    TablesClientFactory tablesClientFactory = Mockito.mock(TablesClientFactory.class);
    JobsClientFactory jobsClientFactory = Mockito.mock(JobsClientFactory.class);
    tasksFactorySnapshotExpiration =
        new OperationTaskFactory<>(
            operationTaskClsSnapshotExpiration, jobsClient, tablesClient, 60000L, 60000L);
    tasksFactoryRetention =
        new OperationTaskFactory<>(
            operationTaskClsRetention, jobsClient, tablesClient, 60000L, 60000L);
    tasksFactoryStatsCollection =
        new OperationTaskFactory<>(
            operationTaskClsStatsCollection, jobsClient, tablesClient, 60000L, 60000L);
    tasksFactoryOrphanFileDeletion =
        new OperationTaskFactory<>(
            operationTaskClsOrphanFileDeletion, jobsClient, tablesClient, 60000L, 60000L);
    operationTaskManagerSnapshotExpiration =
        new OperationTaskManager(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION);
    operationTaskManagerOrphanFileDeletion =
        new OperationTaskManager(JobConf.JobTypeEnum.ORPHAN_FILES_DELETION);
    operationTaskManagerStatsCollection =
        new OperationTaskManager(JobConf.JobTypeEnum.TABLE_STATS_COLLECTION);
    operationTaskManagerRetention = new OperationTaskManager(JobConf.JobTypeEnum.RETENTION);
    jobInfoManagerSnapshotExpiration = new JobInfoManager(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION);
    jobInfoManagerOrphanFileDeletion =
        new JobInfoManager(JobConf.JobTypeEnum.ORPHAN_FILES_DELETION);
    jobInfoManagerStatsCollection = new JobInfoManager(JobConf.JobTypeEnum.TABLE_STATS_COLLECTION);
    jobInfoManagerRetention = new JobInfoManager(JobConf.JobTypeEnum.RETENTION);
    operationTasksBuilderSnapshotExpiration =
        new OperationTasksBuilder(
            tasksFactorySnapshotExpiration,
            tablesClient,
            2,
            operationTaskManagerSnapshotExpiration,
            jobInfoManagerSnapshotExpiration);
    operationTasksBuilderRetention =
        new OperationTasksBuilder(
            tasksFactoryRetention,
            tablesClient,
            2,
            operationTaskManagerRetention,
            jobInfoManagerRetention);
    operationTasksBuilderStatsCollection =
        new OperationTasksBuilder(
            tasksFactoryStatsCollection,
            tablesClient,
            2,
            operationTaskManagerStatsCollection,
            jobInfoManagerStatsCollection);
    operationTasksBuilderOrphanFileDeletion =
        new OperationTasksBuilder(
            tasksFactoryOrphanFileDeletion,
            tablesClient,
            2,
            operationTaskManagerOrphanFileDeletion,
            jobInfoManagerOrphanFileDeletion);
    for (int i = 0; i < dbCount; i++) {
      databases.add("db" + i);
    }
    populateDbAllTables(dbCount, tableCount);
    prepareTableMetadata();
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

  @Test
  public void testCreateStatusOperationTask() {
    Optional<OperationTask<?>> optionalOperationTask =
        operationTasksBuilderSnapshotExpiration.createStatusOperationTask(
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
    Optional<OperationTask<?>> optionalOperationTask =
        operationTasksBuilderSnapshotExpiration.processMetadata(
            tableMetadata, JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION, OperationMode.SUBMIT);
    Assertions.assertNotNull(optionalOperationTask);
    Assertions.assertTrue(optionalOperationTask.isPresent());
    Assertions.assertTrue(optionalOperationTask.get() instanceof TableSnapshotsExpirationTask);
    OperationTask<?> operationTask = optionalOperationTask.get();
    Assertions.assertEquals(OperationMode.SUBMIT, operationTask.operationMode);
    Assertions.assertNull(operationTask.jobId);
    Assertions.assertNotNull(operationTask.jobInfoManager);
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
      for (int j = 0; j < tableCount; j++) {
        GetTableResponseBody getTableResponseBody = getTableResponseBodyList.get(j);
        Mockito.when(tablesClient.mapTableResponseToTableMetadata(getTableResponseBody))
            .thenReturn(getTableResponseToTableMetadata.get(getTableResponseBody));
      }
    }
    Mockito.when(tablesClient.applyDatabaseFilter(Mockito.anyString())).thenReturn(true);
    Mockito.when(tablesClient.applyTableMetadataFilter(Mockito.any(TableMetadata.class)))
        .thenReturn(true);
  }

  @Test
  public void testBuildOperationTaskListInParallelForSnapshotExpiration()
      throws InterruptedException {
    prepareMockitoForParallelFetch();
    operationTasksBuilderSnapshotExpiration.buildOperationTaskListInParallel(
        JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION, OperationMode.SUBMIT);
    // Make sure operation task build is completed
    do {} while (!operationTaskManagerSnapshotExpiration.isDataGenerationCompleted());
    Assertions.assertFalse(operationTaskManagerSnapshotExpiration.isEmpty());
    Assertions.assertEquals(16, operationTaskManagerSnapshotExpiration.getTotalDataCount());
    OperationTask<?> task = operationTaskManagerSnapshotExpiration.getData();
    Assertions.assertNotNull(task);
    Assertions.assertTrue(task instanceof TableSnapshotsExpirationTask);
    Assertions.assertEquals(OperationMode.SUBMIT, task.operationMode);
    Assertions.assertNotNull(task.jobInfoManager);
    Assertions.assertEquals(16, operationTaskManagerSnapshotExpiration.getTotalDataCount());
    Assertions.assertEquals(15, operationTaskManagerSnapshotExpiration.getCurrentDataCount());
  }

  @Test
  public void testBuildOperationTaskListInParallelForSnapshotExpirationVerifyAll()
      throws InterruptedException {
    prepareMockitoForParallelFetch();
    operationTasksBuilderSnapshotExpiration.buildOperationTaskListInParallel(
        JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION, OperationMode.SUBMIT);
    // Make sure operation task build is completed
    int count = 16;
    // Make sure operation task build is completed
    do {} while (!operationTaskManagerSnapshotExpiration.isDataGenerationCompleted());
    // Poll from the queue until there is no element
    do {
      Assertions.assertEquals(count, operationTaskManagerSnapshotExpiration.getCurrentDataCount());
      OperationTask<?> task = operationTaskManagerSnapshotExpiration.getData();
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
    } while (operationTaskManagerSnapshotExpiration.hasNext());
    Assertions.assertTrue(operationTaskManagerSnapshotExpiration.isEmpty());
    Assertions.assertEquals(0, operationTaskManagerSnapshotExpiration.getCurrentDataCount());
    Assertions.assertEquals(16, operationTaskManagerSnapshotExpiration.getTotalDataCount());
  }

  @Test
  public void testBuildOperationTaskListInParallelForOrphanFileDeletion()
      throws InterruptedException {
    prepareMockitoForParallelFetch();
    operationTasksBuilderOrphanFileDeletion.buildOperationTaskListInParallel(
        JobConf.JobTypeEnum.ORPHAN_FILES_DELETION, OperationMode.SUBMIT);
    // Make sure operation task build is completed
    do {} while (!operationTaskManagerOrphanFileDeletion.isDataGenerationCompleted());
    Assertions.assertFalse(operationTaskManagerOrphanFileDeletion.isEmpty());
    Assertions.assertEquals(16, operationTaskManagerOrphanFileDeletion.getCurrentDataCount());
    Assertions.assertEquals(16, operationTaskManagerOrphanFileDeletion.getTotalDataCount());
    OperationTask<?> task = operationTaskManagerOrphanFileDeletion.getData();
    Assertions.assertNotNull(task);
    Assertions.assertTrue(task instanceof TableOrphanFilesDeletionTask);
    Assertions.assertEquals(OperationMode.SUBMIT, task.operationMode);
    Assertions.assertNotNull(task.jobInfoManager);
  }

  @Test
  public void testBuildOperationTaskListInParallelForTableStatsCollection()
      throws InterruptedException {
    prepareMockitoForParallelFetch();
    operationTasksBuilderStatsCollection.buildOperationTaskListInParallel(
        JobConf.JobTypeEnum.TABLE_STATS_COLLECTION, OperationMode.SUBMIT);
    // Make sure operation task build is completed
    do {} while (!operationTaskManagerStatsCollection.isDataGenerationCompleted());
    Assertions.assertFalse(operationTaskManagerStatsCollection.isEmpty());
    Assertions.assertEquals(16, operationTaskManagerStatsCollection.getCurrentDataCount());
    Assertions.assertEquals(16, operationTaskManagerStatsCollection.getTotalDataCount());
    OperationTask<?> task = operationTaskManagerStatsCollection.getData();
    Assertions.assertNotNull(task);
    Assertions.assertTrue(task instanceof TableStatsCollectionTask);
    Assertions.assertEquals(OperationMode.SUBMIT, task.operationMode);
    Assertions.assertNotNull(task.jobInfoManager);
  }

  @Test
  public void testBuildOperationTaskListInParallelForRetention() throws InterruptedException {
    prepareMockitoForParallelFetch();
    operationTasksBuilderRetention.buildOperationTaskListInParallel(
        JobConf.JobTypeEnum.RETENTION, OperationMode.SUBMIT);
    // Make sure operation task build is completed
    do {} while (!operationTaskManagerRetention.isDataGenerationCompleted());
    Assertions.assertFalse(operationTaskManagerRetention.isEmpty());
    Assertions.assertEquals(16, operationTaskManagerRetention.getCurrentDataCount());
    Assertions.assertEquals(16, operationTaskManagerRetention.getTotalDataCount());
    OperationTask<?> task = operationTaskManagerRetention.getData();
    Assertions.assertNotNull(task);
    Assertions.assertTrue(task instanceof TableRetentionTask);
    Assertions.assertEquals(OperationMode.SUBMIT, task.operationMode);
    Assertions.assertNotNull(task.jobInfoManager);
  }

  private void prepareTableMetadata() {
    for (int i = 0; i < dbCount; i++) {
      for (int j = 0; j < tableCount; j++) {
        tableMetadataList.add(getTableMetadata(i, j).get());
      }
    }
  }

  @Test
  public void testBuildOperationTaskListForSnapshotExpiration() {
    Mockito.when(tablesClient.getTableMetadataList()).thenReturn(tableMetadataList);
    List<OperationTask<?>> operationTasks =
        operationTasksBuilderSnapshotExpiration.buildOperationTaskList(
            JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION, null, null, OperationMode.SINGLE);
    Assertions.assertNotNull(operationTasks);
    // Make sure operation task build is completed
    Assertions.assertEquals(16, operationTasks.size());
    for (int i = 0; i < 16; i++) {
      Assertions.assertNotNull(operationTasks.get(i));
      Assertions.assertTrue(operationTasks.get(i) instanceof TableSnapshotsExpirationTask);
      Assertions.assertEquals(OperationMode.SINGLE, operationTasks.get(i).operationMode);
    }
  }

  @Test
  public void testBuildOperationTaskListForOrphanFileDeletion() {
    Mockito.when(tablesClient.getTableMetadataList()).thenReturn(tableMetadataList);
    List<OperationTask<?>> operationTasks =
        operationTasksBuilderOrphanFileDeletion.buildOperationTaskList(
            JobConf.JobTypeEnum.ORPHAN_FILES_DELETION, null, null, OperationMode.SINGLE);
    Assertions.assertNotNull(operationTasks);
    // Make sure operation task build is completed
    Assertions.assertEquals(16, operationTasks.size());
    for (int i = 0; i < 16; i++) {
      Assertions.assertNotNull(operationTasks.get(i));
      Assertions.assertTrue(operationTasks.get(i) instanceof TableOrphanFilesDeletionTask);
      Assertions.assertEquals(OperationMode.SINGLE, operationTasks.get(i).operationMode);
    }
  }

  @AfterEach
  public void reset() {
    operationTaskManagerSnapshotExpiration.resetAll();
    operationTaskManagerOrphanFileDeletion.resetAll();
    operationTaskManagerRetention.resetAll();
    operationTaskManagerStatsCollection.resetAll();
    jobInfoManagerSnapshotExpiration.resetAll();
    jobInfoManagerOrphanFileDeletion.resetAll();
    jobInfoManagerStatsCollection.resetAll();
    jobInfoManagerRetention.resetAll();
  }
}
