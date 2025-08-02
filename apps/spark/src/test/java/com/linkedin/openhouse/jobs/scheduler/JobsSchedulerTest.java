package com.linkedin.openhouse.jobs.scheduler;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.common.OtelEmitter;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.client.model.JobResponseBody;
import com.linkedin.openhouse.jobs.scheduler.tasks.JobInfoManager;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationTask;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationTaskFactory;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationTaskManager;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationTasksBuilder;
import com.linkedin.openhouse.jobs.scheduler.tasks.TableDataLayoutStrategyGenerationTask;
import com.linkedin.openhouse.jobs.scheduler.tasks.TableOrphanFilesDeletionTask;
import com.linkedin.openhouse.jobs.scheduler.tasks.TableRetentionTask;
import com.linkedin.openhouse.jobs.scheduler.tasks.TableSnapshotsExpirationTask;
import com.linkedin.openhouse.jobs.scheduler.tasks.TableStatsCollectionTask;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.jobs.util.RetentionConfig;
import com.linkedin.openhouse.jobs.util.TableMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled(
    "The jobs scheduler test class is disabled as it takes time to run. Enable it to test jobs scheduler locally")
public class JobsSchedulerTest {
  @Mock private TablesClient tablesClient;
  @Mock private JobsClient jobsClient;
  private int dbCount = 4;
  private int tableCount = 4;
  private List<TableMetadata> tableMetadataList = new ArrayList<>();
  private final OtelEmitter otelEmitter = AppsOtelEmitter.getInstance();

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
              TableDataLayoutStrategyGenerationTask.class);
        }
      };

  @BeforeAll
  void setup() {
    MockitoAnnotations.openMocks(this);
    prepareTableMetadata();
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

  private JobsScheduler createJobsScheduler(JobConf.JobTypeEnum jobType) {
    OperationTaskFactory<? extends OperationTask<?>> taskFactory =
        new OperationTaskFactory<>(
            jobTypeToClassMap.get(jobType), jobsClient, tablesClient, 1000L, 2000L, 3000L);
    OperationTaskManager operationTaskManager = new OperationTaskManager(jobType);
    JobInfoManager jobInfoManager = new JobInfoManager(jobType);
    OperationTasksBuilder operationTasksBuilder =
        Mockito.spy(
            new OperationTasksBuilder(
                taskFactory, tablesClient, 2, operationTaskManager, jobInfoManager));
    ThreadPoolExecutor jobExecutors =
        new ThreadPoolExecutor(
            4, 4, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    ThreadPoolExecutor statusExecutors =
        new ThreadPoolExecutor(
            4, 4, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    return new JobsScheduler(
        jobExecutors,
        statusExecutors,
        taskFactory,
        tablesClient,
        jobsClient,
        operationTaskManager,
        jobInfoManager,
        operationTasksBuilder,
        otelEmitter);
  }

  private void mockbuildOperationTaskListInParallel(JobsScheduler jobsScheduler) {
    Mockito.doAnswer(
            invocation -> {
              for (TableMetadata metadata : tableMetadataList) {
                Optional<OperationTask<?>> optionalOperationTask =
                    jobsScheduler
                        .getTasksBuilder()
                        .processMetadata(
                            metadata,
                            invocation.getArgument(0),
                            invocation.getArgument(3),
                            otelEmitter);
                jobsScheduler.getOperationTaskManager().addData(optionalOperationTask.get());
              }
              jobsScheduler.getOperationTaskManager().updateDataGenerationCompletion();
              return null;
            })
        .when(jobsScheduler.getTasksBuilder())
        .buildOperationTaskListInParallel(
            Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
  }

  private void mockbuildOperationTaskList(JobsScheduler jobsScheduler) {
    Mockito.doAnswer(
            invocation -> {
              List<OperationTask<?>> operationTasks = new ArrayList<>();
              for (TableMetadata metadata : tableMetadataList) {
                operationTasks.add(
                    jobsScheduler
                        .getTasksBuilder()
                        .processMetadata(
                            metadata,
                            invocation.getArgument(0),
                            invocation.getArgument(3),
                            otelEmitter)
                        .get());
              }
              return operationTasks;
            })
        .when(jobsScheduler.getTasksBuilder())
        .buildOperationTaskList(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
  }

  private void mockLaunchJobAndPollStatus(
      JobState jobState, JobResponseBody.StateEnum jobResponseState) {
    String jobId = UUID.randomUUID().toString();
    Mockito.when(
            jobsClient.launch(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.anyString(),
                Mockito.anyMap(),
                Mockito.anyList()))
        .thenReturn(Optional.of(jobId));
    JobResponseBody jobResponseBody = Mockito.mock(JobResponseBody.class);
    Mockito.when(jobsClient.getState(jobId)).thenReturn(Optional.of(jobState));
    Mockito.when(jobsClient.getJob(jobId)).thenReturn(Optional.of(jobResponseBody));
    Mockito.when(jobResponseBody.getState()).thenReturn(jobResponseState);
    Mockito.when(jobResponseBody.getJobId()).thenReturn(jobId);
    Mockito.when(jobResponseBody.getStartTimeMs()).thenReturn(0L);
  }

  private void shutDownJobScheduler(JobsScheduler jobsScheduler) {
    jobsScheduler.getJobExecutors().shutdownNow();
    jobsScheduler.getStatusExecutors().shutdownNow();
  }

  @Test
  public void testRunParallelFetchMultiMode() {
    List<JobConf.JobTypeEnum> jobList = Arrays.asList(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION);
    for (JobConf.JobTypeEnum jobType : jobList) {
      mockLaunchJobAndPollStatus(JobState.SUCCEEDED, JobResponseBody.StateEnum.SUCCEEDED);
      JobsScheduler jobsScheduler = createJobsScheduler(jobType);
      mockbuildOperationTaskListInParallel(jobsScheduler);
      jobsScheduler.run(
          jobType,
          jobTypeToClassMap.get(jobType).toString(),
          null,
          false,
          1,
          true,
          true,
          16,
          4,
          1000,
          30,
          15);
      Assertions.assertEquals(16, jobsScheduler.getJobStateCountMap().get(JobState.SUCCEEDED));
      Assertions.assertEquals(7, jobsScheduler.getJobStateCountMap().size());
      shutDownJobScheduler(jobsScheduler);
    }
  }

  @Test
  public void testRunParallelFetchMultiModeFailure() {
    List<JobConf.JobTypeEnum> jobList = Arrays.asList(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION);
    for (JobConf.JobTypeEnum jobType : jobList) {
      mockLaunchJobAndPollStatus(JobState.FAILED, JobResponseBody.StateEnum.FAILED);
      JobsScheduler jobsScheduler = createJobsScheduler(jobType);
      mockbuildOperationTaskListInParallel(jobsScheduler);
      jobsScheduler.run(
          jobType,
          jobTypeToClassMap.get(jobType).toString(),
          null,
          false,
          1,
          true,
          true,
          16,
          4,
          1000,
          30,
          15);
      Assertions.assertEquals(16, jobsScheduler.getJobStateCountMap().get(JobState.FAILED));
      Assertions.assertEquals(7, jobsScheduler.getJobStateCountMap().size());
      shutDownJobScheduler(jobsScheduler);
    }
  }

  @Test
  public void testRunParallelFetchSingleMode() {
    List<JobConf.JobTypeEnum> jobList = Arrays.asList(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION);
    for (JobConf.JobTypeEnum jobType : jobList) {
      mockLaunchJobAndPollStatus(JobState.SUCCEEDED, JobResponseBody.StateEnum.SUCCEEDED);
      JobsScheduler jobsScheduler = createJobsScheduler(jobType);
      mockbuildOperationTaskListInParallel(jobsScheduler);
      jobsScheduler.run(
          jobType,
          jobTypeToClassMap.get(jobType).toString(),
          null,
          false,
          1,
          true,
          false,
          16,
          4,
          1000,
          30,
          15);
      Assertions.assertEquals(16, jobsScheduler.getJobStateCountMap().get(JobState.SUCCEEDED));
      Assertions.assertEquals(7, jobsScheduler.getJobStateCountMap().size());
      shutDownJobScheduler(jobsScheduler);
    }
  }

  @Test
  public void testRunSequentialFetchSingleMode() {
    List<JobConf.JobTypeEnum> jobList = Arrays.asList(JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION);
    for (JobConf.JobTypeEnum jobType : jobList) {
      mockLaunchJobAndPollStatus(JobState.SUCCEEDED, JobResponseBody.StateEnum.SUCCEEDED);
      JobsScheduler jobsScheduler = createJobsScheduler(jobType);
      mockbuildOperationTaskList(jobsScheduler);
      jobsScheduler.run(
          jobType,
          jobTypeToClassMap.get(jobType).toString(),
          null,
          false,
          1,
          false,
          false,
          16,
          4,
          1000,
          30,
          15);
      Assertions.assertEquals(16, jobsScheduler.getJobStateCountMap().get(JobState.SUCCEEDED));
      Assertions.assertEquals(7, jobsScheduler.getJobStateCountMap().size());
      shutDownJobScheduler(jobsScheduler);
    }
  }

  @Test
  void testRegistryIsInitialized() {
    JobsScheduler.main(
        new String[] {
          "--type", TableRetentionTask.OPERATION_TYPE.getValue(),
          "--tablesURL", "http://test.openhouse.com",
          "--jobsURL", "http://test.openhouse.com",
          "--cluster", "unused",
        });
  }
}
