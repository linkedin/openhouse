package com.linkedin.openhouse.jobs.scheduler;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.JobsClientFactory;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.TablesClientFactory;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.client.model.JobResponseBody;
import com.linkedin.openhouse.jobs.scheduler.tasks.JobInfo;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationTask;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationTaskFactory;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationTasksBuilder;
import com.linkedin.openhouse.jobs.scheduler.tasks.TableOrphanFilesDeletionTask;
import com.linkedin.openhouse.jobs.scheduler.tasks.TableRetentionTask;
import com.linkedin.openhouse.jobs.scheduler.tasks.TableSnapshotsExpirationTask;
import com.linkedin.openhouse.jobs.scheduler.tasks.TableStatsCollectionTask;
import com.linkedin.openhouse.jobs.util.Metadata;
import com.linkedin.openhouse.jobs.util.RetentionConfig;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import com.linkedin.openhouse.tables.client.model.GetAllTablesResponseBody;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled(
    "The jobs scheduler test class is disabled as it takes time to run. Enable it to test jobs scheduler locally")
public class JobsSchedulerTest {

  private TablesClient tablesClient;
  private JobsClient jobsClient;
  private Metadata tableMetadata;
  private ThreadPoolExecutor jobExecutors;
  private ThreadPoolExecutor statusExecutors;
  private OperationTasksBuilder operationTasksBuilderSnapshotExpiration;
  private OperationTasksBuilder operationTasksBuilderRetention;
  private OperationTasksBuilder operationTasksBuilderOrphanFileDeletion;
  private OperationTasksBuilder operationTasksBuilderStatsCollection;
  private OperationTaskFactory<TableSnapshotsExpirationTask> tasksFactorySnapshotExpiration;
  private OperationTaskFactory<TableRetentionTask> tasksFactoryRetention;
  private OperationTaskFactory<TableStatsCollectionTask> tasksFactoryStatsCollection;
  private OperationTaskFactory<TableOrphanFilesDeletionTask> tasksFactoryOrphanFileDeletion;
  private Class<TableSnapshotsExpirationTask> operationTaskClsSnapshotExpiration =
      TableSnapshotsExpirationTask.class;
  private Class<TableRetentionTask> operationTaskClsRetention = TableRetentionTask.class;
  private Class<TableStatsCollectionTask> operationTaskClsStatsCollection =
      TableStatsCollectionTask.class;
  private Class<TableOrphanFilesDeletionTask> operationTaskClsOrphanFileDeletion =
      TableOrphanFilesDeletionTask.class;
  private BlockingQueue<OperationTask<?>> operationTaskQueue = new LinkedBlockingQueue<>();
  private BlockingQueue<JobInfo> submittedJobQueue = new LinkedBlockingQueue<>();
  private AtomicLong operationTaskCount = new AtomicLong(0);
  private AtomicBoolean tableMetadataFetchCompleted = new AtomicBoolean(false);
  private Set<String> runningJobs = Collections.synchronizedSet(new HashSet<>());
  private List<String> databases = new ArrayList<>();
  private Map<String, GetAllTablesResponseBody> dbAllTables = new HashMap();
  private Map<GetAllTablesResponseBody, List<GetTableResponseBody>> allTablesBodyToGetTableList =
      new HashMap();
  private Map<GetTableResponseBody, Optional<TableMetadata>> getTableResponseToTableMetadata =
      new HashMap();
  private int dbCount = 4;
  private int tableCount = 4;
  private List<TableMetadata> tableMetadataList = new ArrayList<>();
  private JobsScheduler jobsSchedulerSnapshotExpiration;
  private JobsScheduler jobsSchedulerOrphanFileDeletion;
  private JobResponseBody jobResponseBody;
  private TablesClientFactory tablesClientFactory;
  private JobsClientFactory jobsClientFactory;

  @BeforeAll
  void setup() {
    tablesClient = Mockito.mock(TablesClient.class);
    jobsClient = Mockito.mock(JobsClient.class);
    tableMetadata =
        TableMetadata.builder().dbName("db1").tableName("test_table").isPrimary(true).build();
    tablesClientFactory = Mockito.mock(TablesClientFactory.class);
    jobsClientFactory = Mockito.mock(JobsClientFactory.class);
    tasksFactorySnapshotExpiration =
        new OperationTaskFactory<>(
            operationTaskClsSnapshotExpiration,
            jobsClientFactory,
            tablesClientFactory,
            60000L,
            60000L);
    tasksFactoryRetention =
        new OperationTaskFactory<>(
            operationTaskClsRetention, jobsClientFactory, tablesClientFactory, 60000L, 60000L);
    tasksFactoryStatsCollection =
        new OperationTaskFactory<>(
            operationTaskClsStatsCollection,
            jobsClientFactory,
            tablesClientFactory,
            60000L,
            60000L);
    tasksFactoryOrphanFileDeletion =
        new OperationTaskFactory<>(
            operationTaskClsOrphanFileDeletion,
            jobsClientFactory,
            tablesClientFactory,
            60000L,
            60000L);
    for (int i = 0; i < dbCount; i++) {
      databases.add("db" + i);
    }
    populateDbAllTables(dbCount, tableCount);
    prepareTableMetadata();
    jobExecutors =
        new ThreadPoolExecutor(
            2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    statusExecutors =
        new ThreadPoolExecutor(
            2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    jobsSchedulerSnapshotExpiration =
        new JobsScheduler(
            jobExecutors,
            statusExecutors,
            tasksFactorySnapshotExpiration,
            tablesClientFactory.create(),
            new LinkedBlockingQueue<>(),
            new LinkedBlockingQueue<>());
    jobsSchedulerOrphanFileDeletion =
        new JobsScheduler(
            jobExecutors,
            statusExecutors,
            tasksFactoryOrphanFileDeletion,
            tablesClientFactory.create(),
            new LinkedBlockingQueue<>(),
            new LinkedBlockingQueue<>());
    operationTasksBuilderSnapshotExpiration =
        new OperationTasksBuilder(
            tasksFactorySnapshotExpiration,
            tablesClient,
            jobsSchedulerSnapshotExpiration.operationTaskQueue,
            2,
            jobsSchedulerSnapshotExpiration.tableMetadataFetchCompleted,
            jobsSchedulerSnapshotExpiration.operationTaskCount,
            jobsSchedulerSnapshotExpiration.submittedJobQueue,
            jobsSchedulerSnapshotExpiration.runningJobs);
    operationTasksBuilderOrphanFileDeletion =
        new OperationTasksBuilder(
            tasksFactoryOrphanFileDeletion,
            tablesClient,
            jobsSchedulerOrphanFileDeletion.operationTaskQueue,
            2,
            jobsSchedulerOrphanFileDeletion.tableMetadataFetchCompleted,
            jobsSchedulerOrphanFileDeletion.operationTaskCount,
            jobsSchedulerOrphanFileDeletion.submittedJobQueue,
            jobsSchedulerOrphanFileDeletion.runningJobs);
    jobResponseBody = Mockito.mock(JobResponseBody.class);
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
            .creator("openhouse")
            .retentionConfig(RetentionConfig.builder().columnName("column").build())
            .build());
  }

  private void prepareTableMetadata() {
    for (int i = 0; i < dbCount; i++) {
      for (int j = 0; j < tableCount; j++) {
        tableMetadataList.add(getTableMetadata(i, j).get());
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
  public void testRunSnapshotExpirationParallelFetchMultiMode() {
    String jobId = UUID.randomUUID().toString();
    Mockito.when(tablesClientFactory.create()).thenReturn(tablesClient);
    Mockito.when(jobsClientFactory.create()).thenReturn(jobsClient);
    Mockito.when(
            jobsClient.launch(
                Mockito.anyString(),
                Mockito.eq(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION),
                Mockito.anyString(),
                Mockito.anyMap(),
                Mockito.anyList()))
        .thenReturn(Optional.of(jobId));
    Mockito.when(jobsClient.getState(jobId)).thenReturn(Optional.of(JobState.SUCCEEDED));
    Mockito.when(jobsClient.getJob(jobId)).thenReturn(Optional.of(jobResponseBody));
    Mockito.when(jobResponseBody.getState()).thenReturn(JobResponseBody.StateEnum.SUCCEEDED);
    Mockito.when(jobResponseBody.getJobId()).thenReturn(jobId);
    Mockito.when(jobResponseBody.getStartTimeMs()).thenReturn(0L);
    prepareMockitoForParallelFetch();
    jobsSchedulerSnapshotExpiration.run(
        JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION,
        operationTaskClsSnapshotExpiration.toString(),
        null,
        operationTasksBuilderSnapshotExpiration,
        false,
        1,
        true,
        4,
        true,
        16,
        2,
        1000,
        30,
        15);
    Assertions.assertEquals(
        16, jobsSchedulerSnapshotExpiration.jobStateCountMap.get(JobState.SUCCEEDED));
    Assertions.assertEquals(7, jobsSchedulerSnapshotExpiration.jobStateCountMap.size());
  }

  @Test
  public void testRunSnapshotExpirationParallelFetchMultiModeFailure() {
    String jobId = UUID.randomUUID().toString();
    Mockito.when(tablesClientFactory.create()).thenReturn(tablesClient);
    Mockito.when(jobsClientFactory.create()).thenReturn(jobsClient);
    Mockito.when(
            jobsClient.launch(
                Mockito.anyString(),
                Mockito.eq(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION),
                Mockito.anyString(),
                Mockito.anyMap(),
                Mockito.anyList()))
        .thenReturn(Optional.of(jobId));
    Mockito.when(jobsClient.getState(jobId)).thenReturn(Optional.of(JobState.FAILED));
    Mockito.when(jobsClient.getJob(jobId)).thenReturn(Optional.of(jobResponseBody));
    Mockito.when(jobResponseBody.getState()).thenReturn(JobResponseBody.StateEnum.FAILED);
    Mockito.when(jobResponseBody.getJobId()).thenReturn(jobId);
    Mockito.when(jobResponseBody.getStartTimeMs()).thenReturn(0L);
    prepareMockitoForParallelFetch();
    jobsSchedulerSnapshotExpiration.run(
        JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION,
        operationTaskClsSnapshotExpiration.toString(),
        null,
        operationTasksBuilderSnapshotExpiration,
        false,
        1,
        true,
        4,
        true,
        16,
        2,
        1000,
        30,
        15);
    Assertions.assertEquals(
        16, jobsSchedulerSnapshotExpiration.jobStateCountMap.get(JobState.FAILED));
    Assertions.assertEquals(7, jobsSchedulerSnapshotExpiration.jobStateCountMap.size());
  }

  @Test
  public void testRunSnapshotExpirationParallelFetchSingleMode() {
    String jobId = UUID.randomUUID().toString();
    Mockito.when(tablesClientFactory.create()).thenReturn(tablesClient);
    Mockito.when(jobsClientFactory.create()).thenReturn(jobsClient);
    Mockito.when(
            jobsClient.launch(
                Mockito.anyString(),
                Mockito.eq(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION),
                Mockito.anyString(),
                Mockito.anyMap(),
                Mockito.anyList()))
        .thenReturn(Optional.of(jobId));
    Mockito.when(jobsClient.getState(jobId)).thenReturn(Optional.of(JobState.SUCCEEDED));
    Mockito.when(jobsClient.getJob(jobId)).thenReturn(Optional.of(jobResponseBody));
    Mockito.when(jobResponseBody.getState()).thenReturn(JobResponseBody.StateEnum.SUCCEEDED);
    Mockito.when(jobResponseBody.getJobId()).thenReturn(jobId);
    Mockito.when(jobResponseBody.getStartTimeMs()).thenReturn(0L);
    prepareMockitoForParallelFetch();
    jobsSchedulerSnapshotExpiration.run(
        JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION,
        operationTaskClsSnapshotExpiration.toString(),
        null,
        operationTasksBuilderSnapshotExpiration,
        false,
        1,
        true,
        4,
        false,
        16,
        2,
        1000,
        30,
        15);
    Assertions.assertEquals(
        16, jobsSchedulerSnapshotExpiration.jobStateCountMap.get(JobState.SUCCEEDED));
    Assertions.assertEquals(7, jobsSchedulerSnapshotExpiration.jobStateCountMap.size());
  }

  @Test
  public void testRunSnapshotExpirationSequentialFetchSingleMode() {
    String jobId = UUID.randomUUID().toString();
    Mockito.when(tablesClientFactory.create()).thenReturn(tablesClient);
    Mockito.when(jobsClientFactory.create()).thenReturn(jobsClient);
    Mockito.when(tablesClient.getTableMetadataList()).thenReturn(tableMetadataList);
    Mockito.when(
            jobsClient.launch(
                Mockito.anyString(),
                Mockito.eq(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION),
                Mockito.anyString(),
                Mockito.anyMap(),
                Mockito.anyList()))
        .thenReturn(Optional.of(jobId));
    Mockito.when(jobsClient.getState(jobId)).thenReturn(Optional.of(JobState.SUCCEEDED));
    Mockito.when(jobsClient.getJob(jobId)).thenReturn(Optional.of(jobResponseBody));
    Mockito.when(jobResponseBody.getState()).thenReturn(JobResponseBody.StateEnum.SUCCEEDED);
    Mockito.when(jobResponseBody.getJobId()).thenReturn(jobId);
    Mockito.when(jobResponseBody.getStartTimeMs()).thenReturn(0L);
    prepareMockitoForParallelFetch();
    jobsSchedulerSnapshotExpiration.run(
        JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION,
        operationTaskClsSnapshotExpiration.toString(),
        null,
        operationTasksBuilderSnapshotExpiration,
        false,
        1,
        false,
        4,
        false,
        16,
        2,
        1000,
        30,
        15);
    Assertions.assertEquals(
        16, jobsSchedulerSnapshotExpiration.jobStateCountMap.get(JobState.SUCCEEDED));
    Assertions.assertEquals(7, jobsSchedulerSnapshotExpiration.jobStateCountMap.size());
  }

  @Test
  public void testRunOrphanFileDeletionParallelFetchMultiMode() {
    String jobId = UUID.randomUUID().toString();
    Mockito.when(tablesClientFactory.create()).thenReturn(tablesClient);
    Mockito.when(jobsClientFactory.create()).thenReturn(jobsClient);
    Mockito.when(
            jobsClient.launch(
                Mockito.anyString(),
                Mockito.eq(JobConf.JobTypeEnum.ORPHAN_FILES_DELETION),
                Mockito.anyString(),
                Mockito.anyMap(),
                Mockito.anyList()))
        .thenReturn(Optional.of(jobId));
    Mockito.when(jobsClient.getState(jobId)).thenReturn(Optional.of(JobState.SUCCEEDED));
    Mockito.when(jobsClient.getJob(jobId)).thenReturn(Optional.of(jobResponseBody));
    Mockito.when(jobResponseBody.getState()).thenReturn(JobResponseBody.StateEnum.SUCCEEDED);
    Mockito.when(jobResponseBody.getJobId()).thenReturn(jobId);
    Mockito.when(jobResponseBody.getStartTimeMs()).thenReturn(0L);
    prepareMockitoForParallelFetch();
    jobsSchedulerOrphanFileDeletion.run(
        JobConf.JobTypeEnum.ORPHAN_FILES_DELETION,
        operationTaskClsOrphanFileDeletion.toString(),
        null,
        operationTasksBuilderOrphanFileDeletion,
        false,
        1,
        true,
        4,
        true,
        16,
        2,
        1000,
        30,
        15);
    Assertions.assertEquals(7, jobsSchedulerOrphanFileDeletion.jobStateCountMap.size());
    Assertions.assertEquals(
        16, jobsSchedulerOrphanFileDeletion.jobStateCountMap.get(JobState.SUCCEEDED));
  }

  @AfterEach
  public void reset() {
    operationTaskQueue.clear();
    runningJobs.clear();
    tableMetadataFetchCompleted.set(false);
    submittedJobQueue.clear();
    operationTaskCount.set(0);
    jobExecutors =
        new ThreadPoolExecutor(
            2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    statusExecutors =
        new ThreadPoolExecutor(
            2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
  }

  @Test
  void testRegistryIsInitialized() {
    JobsScheduler.main(
        new String[] {
          "--type", TableRetentionTask.OPERATION_TYPE.getValue(),
          "--tablesURL", "http://test.openhouse.com",
          "--jobsURL", "unused",
          "--cluster", "unused",
        });
  }
}
