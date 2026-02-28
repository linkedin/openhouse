package com.linkedin.openhouse.jobs.scheduler;

import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.client.model.JobResponseBody;
import com.linkedin.openhouse.jobs.scheduler.tasks.BaseDataManager;
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
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JobsSchedulerTest {
  @Mock private TablesClient tablesClient;
  @Mock private JobsClient jobsClient;
  private int dbCount = 4;
  private int tableCount = 4;
  private List<TableMetadata> tableMetadataList = new ArrayList<>();
  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

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

  private ThreadPoolExecutor createThreadPool(int size) {
    return new ThreadPoolExecutor(
        size, size, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
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
    return new JobsScheduler(
        createThreadPool(4),
        createThreadPool(4),
        taskFactory,
        tablesClient,
        jobsClient,
        operationTaskManager,
        jobInfoManager,
        operationTasksBuilder,
        otelEmitter);
  }

  /**
   * Creates a mock OtelEmitter that delegates executeWithStats to the callable without
   * synchronization. The real AppsOtelEmitter has synchronized methods that serialize all task
   * execution, preventing parallel operation in tests with delayed mocks.
   */
  private OtelEmitter createMockOtelEmitter() {
    OtelEmitter mockOtelEmitter = Mockito.mock(OtelEmitter.class);
    try {
      Mockito.when(
              mockOtelEmitter.executeWithStats(
                  Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any()))
          .thenAnswer(
              invocation -> {
                Callable<?> callable = invocation.getArgument(0);
                return callable.call();
              });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return mockOtelEmitter;
  }

  /**
   * Creates a spied JobInfoManager with a shorter getData() poll timeout (1 second instead of the
   * default 1 minute). This prevents the status polling loop from hanging after shutdown drains all
   * items from the queue.
   */
  private JobInfoManager createSpiedJobInfoManager(JobConf.JobTypeEnum jobType) {
    JobInfoManager jobInfoManager = Mockito.spy(new JobInfoManager(jobType));
    try {
      Mockito.doAnswer(
              invocation -> {
                java.lang.reflect.Field queueField =
                    BaseDataManager.class.getDeclaredField("dataQueue");
                queueField.setAccessible(true);
                java.util.concurrent.BlockingQueue<?> queue =
                    (java.util.concurrent.BlockingQueue<?>) queueField.get(invocation.getMock());
                return queue.poll(1, TimeUnit.SECONDS);
              })
          .when(jobInfoManager)
          .getData();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return jobInfoManager;
  }

  private void mockbuildOperationTaskListInParallel(
      JobsScheduler jobsScheduler, OtelEmitter emitter) {
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
                            emitter);
                jobsScheduler.getOperationTaskManager().addData(optionalOperationTask.get());
              }
              jobsScheduler.getOperationTaskManager().updateDataGenerationCompletion();
              return null;
            })
        .when(jobsScheduler.getTasksBuilder())
        .buildOperationTaskListInParallel(
            Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
  }

  private void mockbuildOperationTaskListInParallel(JobsScheduler jobsScheduler) {
    mockbuildOperationTaskListInParallel(jobsScheduler, otelEmitter);
  }

  private void mockbuildOperationTaskList(JobsScheduler jobsScheduler, OtelEmitter emitter) {
    Mockito.doAnswer(
            invocation -> {
              List<OperationTask<?>> operationTasks = new ArrayList<>();
              for (TableMetadata metadata : tableMetadataList) {
                operationTasks.add(
                    jobsScheduler
                        .getTasksBuilder()
                        .processMetadata(
                            metadata, invocation.getArgument(0), invocation.getArgument(3), emitter)
                        .get());
              }
              return operationTasks;
            })
        .when(jobsScheduler.getTasksBuilder())
        .buildOperationTaskList(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
  }

  private void mockbuildOperationTaskList(JobsScheduler jobsScheduler) {
    mockbuildOperationTaskList(jobsScheduler, otelEmitter);
  }

  /**
   * Mocks jobsClient.launch() to return a fixed jobId (instant), and mocks getState/getJob to
   * return the specified state. Used for tests that don't need launch delays.
   */
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
    mockStatusPolling(jobId, jobState, jobResponseState);
  }

  /**
   * Mocks jobsClient.launch() with a delay to simulate Livy submission time, returning unique job
   * IDs. Returns an AtomicInteger that tracks the number of launches.
   */
  private AtomicInteger mockLaunchJobWithDelay(int delayMs) {
    AtomicInteger launchCount = new AtomicInteger(0);
    Mockito.when(
            jobsClient.launch(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.anyString(),
                Mockito.anyMap(),
                Mockito.anyList()))
        .thenAnswer(
            invocation -> {
              launchCount.incrementAndGet();
              Thread.sleep(delayMs);
              return Optional.of(UUID.randomUUID().toString());
            });
    return launchCount;
  }

  /**
   * Mocks jobsClient.getState() and jobsClient.getJob() to return the specified job state. Uses
   * anyString() matchers when jobId is null, or exact jobId match otherwise.
   */
  private void mockStatusPolling(
      String jobId, JobState jobState, JobResponseBody.StateEnum jobResponseState) {
    JobResponseBody jobResponseBody = Mockito.mock(JobResponseBody.class);
    if (jobId != null) {
      Mockito.when(jobsClient.getState(jobId)).thenReturn(Optional.of(jobState));
      Mockito.when(jobsClient.getJob(jobId)).thenReturn(Optional.of(jobResponseBody));
    } else {
      Mockito.when(jobsClient.getState(Mockito.anyString())).thenReturn(Optional.of(jobState));
      Mockito.when(jobsClient.getJob(Mockito.anyString())).thenReturn(Optional.of(jobResponseBody));
    }
    Mockito.when(jobResponseBody.getState()).thenReturn(jobResponseState);
    Mockito.when(jobResponseBody.getJobId()).thenReturn(jobId != null ? jobId : "mock-job");
    Mockito.when(jobResponseBody.getStartTimeMs()).thenReturn(0L);
  }

  private void shutDownJobScheduler(JobsScheduler jobsScheduler) {
    jobsScheduler.getJobExecutors().shutdownNow();
    if (jobsScheduler.getStatusExecutors() != null) {
      jobsScheduler.getStatusExecutors().shutdownNow();
    }
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
  public void testSlaTimeoutCancelsRemainingFutures() {
    JobConf.JobTypeEnum jobType = JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION;
    JobsScheduler scheduler = createJobsScheduler(jobType);
    // Initialize jobStateCountMap via run's init logic
    Arrays.stream(JobState.values()).forEach(s -> scheduler.getJobStateCountMap().put(s, 0));

    List<OperationTask<?>> tasks = new ArrayList<>();
    List<Future<Optional<JobState>>> futures = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      OperationTask<?> task = Mockito.mock(OperationTask.class);
      Mockito.when(task.getJobId()).thenReturn("job-" + i);
      tasks.add(task);
      futures.add(new CompletableFuture<>()); // never completed
    }

    // tasksWaitHours=0 triggers immediate SLA timeout
    scheduler.updateJobStateFromTaskFutures(
        jobType, scheduler.getJobExecutors(), tasks, futures, System.currentTimeMillis(), 0, false);

    Assertions.assertEquals(3, scheduler.getJobStateCountMap().get(JobState.CANCELLED));
    for (Future<Optional<JobState>> f : futures) {
      Assertions.assertTrue(f.isCancelled());
    }
    shutDownJobScheduler(scheduler);
  }

  @Test
  public void testOutOfOrderFutureCompletion() {
    JobConf.JobTypeEnum jobType = JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION;
    JobsScheduler scheduler = createJobsScheduler(jobType);
    Arrays.stream(JobState.values()).forEach(s -> scheduler.getJobStateCountMap().put(s, 0));

    List<OperationTask<?>> tasks = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      OperationTask<?> task = Mockito.mock(OperationTask.class);
      Mockito.when(task.getJobId()).thenReturn("job-" + i);
      tasks.add(task);
    }

    // Future 0 completes after 2s; futures 1 and 2 are already done
    CompletableFuture<Optional<JobState>> slowFuture = new CompletableFuture<>();
    new Thread(
            () -> {
              try {
                Thread.sleep(2000);
              } catch (InterruptedException ignored) {
              }
              slowFuture.complete(Optional.of(JobState.SUCCEEDED));
            })
        .start();

    List<Future<Optional<JobState>>> futures =
        Arrays.asList(
            slowFuture,
            CompletableFuture.completedFuture(Optional.of(JobState.SUCCEEDED)),
            CompletableFuture.completedFuture(Optional.of(JobState.SUCCEEDED)));

    scheduler.updateJobStateFromTaskFutures(
        jobType, scheduler.getJobExecutors(), tasks, futures, System.currentTimeMillis(), 1, false);

    Assertions.assertEquals(3, scheduler.getJobStateCountMap().get(JobState.SUCCEEDED));
    shutDownJobScheduler(scheduler);
  }

  @Test
  public void testGracefulShutdownDrainsRemainingFutures() {
    JobConf.JobTypeEnum jobType = JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION;
    JobsScheduler scheduler = createJobsScheduler(jobType);
    Arrays.stream(JobState.values()).forEach(s -> scheduler.getJobStateCountMap().put(s, 0));

    List<OperationTask<?>> tasks = new ArrayList<>();
    List<Future<Optional<JobState>>> futures = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      OperationTask<?> task = Mockito.mock(OperationTask.class);
      Mockito.when(task.getJobId()).thenReturn("job-" + i);
      tasks.add(task);
      futures.add(new CompletableFuture<>()); // never completed
    }

    // Initiate graceful shutdown before polling futures
    scheduler.initiateGracefulShutdown();

    // updateJobStateFromTaskFutures should detect shutdown and drain/cancel futures
    scheduler.updateJobStateFromTaskFutures(
        jobType,
        scheduler.getJobExecutors(),
        tasks,
        futures,
        System.currentTimeMillis(),
        12,
        false);

    Assertions.assertEquals(3, scheduler.getJobStateCountMap().get(JobState.CANCELLED));
    for (Future<Optional<JobState>> f : futures) {
      Assertions.assertTrue(f.isCancelled());
    }
    shutDownJobScheduler(scheduler);
  }

  /**
   * Integration test for graceful shutdown in sequential mode. Simulates a production-like
   * environment where jobs are submitted to a thread pool with realistic delays. A graceful
   * shutdown is triggered mid-execution to verify:
   *
   * <ul>
   *   <li>The scheduler exits cleanly without hanging
   *   <li>Already-running jobs complete and report their state
   *   <li>Pending jobs are cancelled
   * </ul>
   */
  @Test
  public void testGracefulShutdownDuringExecution() throws InterruptedException {
    JobConf.JobTypeEnum jobType = JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION;
    OtelEmitter mockOtelEmitter = createMockOtelEmitter();
    AtomicInteger launchCount = mockLaunchJobWithDelay(300);
    mockStatusPolling(null, JobState.SUCCEEDED, JobResponseBody.StateEnum.SUCCEEDED);

    // Create scheduler with short poll interval (100ms) for faster status checks
    OperationTaskFactory<? extends OperationTask<?>> taskFactory =
        new OperationTaskFactory<>(
            jobTypeToClassMap.get(jobType), jobsClient, tablesClient, 100L, 2000L, 3000L);
    OperationTaskManager operationTaskManager = new OperationTaskManager(jobType);
    JobInfoManager jobInfoManager = new JobInfoManager(jobType);
    OperationTasksBuilder operationTasksBuilder =
        Mockito.spy(
            new OperationTasksBuilder(
                taskFactory, tablesClient, 2, operationTaskManager, jobInfoManager));
    ThreadPoolExecutor jobExecutors = createThreadPool(4);
    JobsScheduler scheduler =
        new JobsScheduler(
            jobExecutors,
            null,
            taskFactory,
            tablesClient,
            jobsClient,
            operationTaskManager,
            jobInfoManager,
            operationTasksBuilder,
            mockOtelEmitter);
    mockbuildOperationTaskList(scheduler, mockOtelEmitter);

    // Run scheduler in sequential mode (parallelMetadataFetch=false, multiOperation=false)
    Thread schedulerThread =
        new Thread(
            () ->
                scheduler.run(
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
                    15));
    schedulerThread.start();

    // Wait for some jobs to launch, then trigger graceful shutdown
    while (launchCount.get() < 4) {
      Thread.sleep(50);
    }
    scheduler.initiateGracefulShutdown();

    // Scheduler should exit cleanly
    schedulerThread.join(30_000);
    Assertions.assertFalse(schedulerThread.isAlive(), "Scheduler should have exited cleanly");

    // Some jobs should have completed, some should have been cancelled
    int succeeded = scheduler.getJobStateCountMap().get(JobState.SUCCEEDED);
    int cancelled = scheduler.getJobStateCountMap().get(JobState.CANCELLED);
    Assertions.assertTrue(succeeded > 0, "Some jobs should have completed successfully");
    Assertions.assertTrue(cancelled > 0, "Some jobs should have been cancelled");
    Assertions.assertTrue(succeeded + cancelled <= 16, "Total should not exceed number of tables");

    shutDownJobScheduler(scheduler);
  }

  /**
   * Integration test for graceful shutdown in multi-operation mode. In this mode, job submission
   * (SUBMIT) and status polling (POLL) are decoupled into separate pipelines connected by the
   * jobInfoManager queue. Verifies:
   *
   * <ul>
   *   <li>The scheduler exits cleanly without hanging
   *   <li>Already-submitted jobs complete status polling and report their state
   *   <li>Shutdown stops further job submissions
   * </ul>
   */
  @Test
  public void testGracefulShutdownMultiOperationMode() throws InterruptedException {
    JobConf.JobTypeEnum jobType = JobConf.JobTypeEnum.SNAPSHOTS_EXPIRATION;
    OtelEmitter mockOtelEmitter = createMockOtelEmitter();
    AtomicInteger launchCount = mockLaunchJobWithDelay(300);
    mockStatusPolling(null, JobState.SUCCEEDED, JobResponseBody.StateEnum.SUCCEEDED);

    // Create scheduler with both job and status executors for multi-operation mode
    OperationTaskFactory<? extends OperationTask<?>> taskFactory =
        new OperationTaskFactory<>(
            jobTypeToClassMap.get(jobType), jobsClient, tablesClient, 100L, 2000L, 3000L);
    OperationTaskManager operationTaskManager = new OperationTaskManager(jobType);
    JobInfoManager jobInfoManager = createSpiedJobInfoManager(jobType);
    OperationTasksBuilder operationTasksBuilder =
        Mockito.spy(
            new OperationTasksBuilder(
                taskFactory, tablesClient, 2, operationTaskManager, jobInfoManager));
    ThreadPoolExecutor jobExecutors = createThreadPool(4);
    ThreadPoolExecutor statusExecutors = createThreadPool(4);
    JobsScheduler scheduler =
        new JobsScheduler(
            jobExecutors,
            statusExecutors,
            taskFactory,
            tablesClient,
            jobsClient,
            operationTaskManager,
            jobInfoManager,
            operationTasksBuilder,
            mockOtelEmitter);
    mockbuildOperationTaskListInParallel(scheduler, mockOtelEmitter);

    // Run scheduler in multi-operation mode (parallelMetadataFetch=true, multiOperation=true)
    Thread schedulerThread =
        new Thread(
            () ->
                scheduler.run(
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
                    15));
    schedulerThread.start();

    // Wait for some jobs to launch, then trigger graceful shutdown
    while (launchCount.get() < 4) {
      Thread.sleep(50);
    }
    scheduler.initiateGracefulShutdown();

    // Scheduler should exit cleanly. The status polling loop may block for up to 1s on the
    // spied getData() poll timeout after the jobInfoManager queue is drained, so allow extra time.
    schedulerThread.join(30_000);
    Assertions.assertFalse(schedulerThread.isAlive(), "Scheduler should have exited cleanly");

    // In multi-operation mode, state counts come from status polling (submit side skips counts).
    // Some jobs should have made it through the full submit->status pipeline.
    int succeeded = scheduler.getJobStateCountMap().get(JobState.SUCCEEDED);
    int cancelled = scheduler.getJobStateCountMap().get(JobState.CANCELLED);
    Assertions.assertTrue(succeeded > 0, "Some jobs should have completed through status polling");
    Assertions.assertTrue(succeeded < 16, "Not all jobs should have completed due to shutdown");
    Assertions.assertTrue(succeeded + cancelled <= 16, "Total should not exceed number of tables");
    Assertions.assertTrue(launchCount.get() < 16, "Shutdown should have stopped further launches");

    shutDownJobScheduler(scheduler);
  }

  @Test
  void testRegistryIsInitialized() {
    // This test is designed to fail if the JobsScheduler class cannot be initialized.
    // The static initializer block in JobsScheduler performs a reflective scan of OperationTask
    // subclasses. If any of these subclasses are abstract and not properly excluded, the
    // class initialization will fail with an ExceptionInInitializerError.
    Assertions.assertDoesNotThrow(
        () -> {
          Class.forName(JobsScheduler.class.getName());
        },
        "JobsScheduler class should be initialized without throwing an exception");
  }
}
