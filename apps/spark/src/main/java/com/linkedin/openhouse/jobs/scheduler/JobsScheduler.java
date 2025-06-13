package com.linkedin.openhouse.jobs.scheduler;

import com.linkedin.openhouse.cluster.storage.filesystem.ParameterizedHdfsStorageProvider;
import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.jobs.client.JobsClient;
import com.linkedin.openhouse.jobs.client.JobsClientFactory;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.TablesClientFactory;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.scheduler.tasks.JobInfo;
import com.linkedin.openhouse.jobs.scheduler.tasks.JobInfoManager;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationMode;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationTask;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationTaskFactory;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationTaskManager;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationTasksBuilder;
import com.linkedin.openhouse.jobs.scheduler.tasks.TableDirectoryOperationTask;
import com.linkedin.openhouse.jobs.scheduler.tasks.TableOperationTask;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.DatabaseTableFilter;
import com.linkedin.openhouse.jobs.util.OtelConfig;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.math.NumberUtils;
import org.reflections.Reflections;

/**
 * Class for Scheduler app to run maintenance operations for individual tables, e.g.
 * orphan_file_deletion, snapshots_expiration, retention, compaction, etc. Those operations are
 * wrapped into Spark apps/jobs with a specific type. The full list of supported types are in {@link
 * com.linkedin.openhouse.jobs.client.model.JobConf.JobTypeEnum}.
 *
 * <p>The list of operations are subclasses of {@link OperationTask} The scheduler maintains an
 * operation registry mapping an operation name to a specific {@link OperationTask} subclass.
 *
 * <p>The scheduler has the following responsibilities: 1. Discover tables and fetch metadata needed
 * for the operations, e.g. partition column and retention TTL. 2. Find tables that require an
 * operation to run, e.g. non-partitioned tables don't need retention operation, or tables on which
 * the operation has already run within past 1 day. 3. Run operations on tables with bounded
 * parallelism and handle the lifecycle of the jobs that run those operations.
 */
@Slf4j
public class JobsScheduler {
  private static final int TASKS_WAIT_HOURS_DEFAULT = 12;
  private static final int MAX_NUM_CONCURRENT_JOBS_DEFAULT = 40;
  private static final int TABLE_MIN_AGE_THRESHOLD_HOURS_DEFAULT = 72;
  private static final boolean PARALLEL_METADATA_FETCH_MODE_DEFAULT = false;
  private static final int MAX_NUM_CONCURRENT_METADATA_FETCH_THREAD_DEFAULT = 20;
  private static final int QUEUE_POLL_TIMEOUT_MINUTES_DEFAULT = 1;
  private static final int MAX_JOBS_SUBMITTER_DEFAULT = 50;
  private static final int MAX_JOBS_POLLER_DEFAULT = 50;
  private static final int JOB_SUBMISSION_PAUSE_IN_MILLIS_DEFAULT = 60000;
  private static final int SUBMIT_OPERATION_PRE_SLA_GRACE_PERIOD_MINUTES_DEFAULT = 30;
  private static final int STATUS_OPERATION_PRE_SLA_GRACE_PERIOD_MINUTES_DEFAULT = 15;

  private static final Map<String, Class<? extends OperationTask>> OPERATIONS_REGISTRY =
      new HashMap<>();
  private static final Meter METER = OtelConfig.getMeter(JobsScheduler.class.getName());

  static {
    Reflections reflections = new Reflections(JobsScheduler.class.getPackage().getName());
    reflections
        .getSubTypesOf(OperationTask.class)
        .forEach(
            subclass -> {
              if (!subclass.equals(TableOperationTask.class)
                  && !subclass.equals(TableDirectoryOperationTask.class)) {
                try {
                  Field nameField = subclass.getDeclaredField("OPERATION_TYPE");
                  OPERATIONS_REGISTRY.put(
                      ((JobConf.JobTypeEnum) nameField.get(null)).name(), subclass);
                } catch (NoSuchFieldException | IllegalAccessException e) {
                  throw new RuntimeException("Cannot access OPERATION_TYPE field");
                }
              }
            });
  }

  private static final String SUPPORTED_OPERATIONS_STRING =
      String.join(",", OPERATIONS_REGISTRY.keySet());

  private final ThreadPoolExecutor jobExecutors;
  private final ThreadPoolExecutor statusExecutors;
  private final OperationTaskFactory<? extends OperationTask> taskFactory;
  private final TablesClient tablesClient;
  private final JobsClient jobsClient;
  private AtomicBoolean jobLaunchTasksSubmissionCompleted = new AtomicBoolean(false);
  private AtomicBoolean jobStatusTasksSubmissionCompleted = new AtomicBoolean(false);
  protected final OperationTaskManager operationTaskManager;
  protected final JobInfoManager jobInfoManager;
  private OperationTasksBuilder builder = null;
  protected Map<JobState, Integer> jobStateCountMap = new ConcurrentHashMap<>();

  public JobsScheduler(
      ThreadPoolExecutor jobExecutors,
      ThreadPoolExecutor statusExecutors,
      OperationTaskFactory<? extends OperationTask> taskFactory,
      TablesClient tablesClient,
      JobsClient jobsClient,
      OperationTaskManager operationTaskManager,
      JobInfoManager jobInfoManager) {
    this.jobExecutors = jobExecutors;
    this.statusExecutors = statusExecutors;
    this.taskFactory = taskFactory;
    this.tablesClient = tablesClient;
    this.jobsClient = jobsClient;
    this.operationTaskManager = operationTaskManager;
    this.jobInfoManager = jobInfoManager;
  }

  public static void main(String[] args) {
    log.info("Starting scheduler");
    CommandLine cmdLine = parseArgs(args);
    JobConf.JobTypeEnum operationType = getOperationJobType(cmdLine);
    Class<? extends OperationTask> operationTaskCls = getOperationTaskCls(operationType.toString());
    TablesClientFactory tablesClientFactory = getTablesClientFactory(cmdLine);
    TablesClient tablesClient = tablesClientFactory.create();
    JobsClientFactory jobsClientFactory = getJobsClientFactory(cmdLine);
    JobsClient jobsClient = jobsClientFactory.create();
    Properties properties = getAdditionalProperties(cmdLine);
    OperationTaskFactory<? extends OperationTask> tasksFactory =
        new OperationTaskFactory<>(
            operationTaskCls,
            jobsClient,
            tablesClient,
            NumberUtils.toLong(
                cmdLine.getOptionValue("taskPollIntervalMs"),
                OperationTask.POLL_INTERVAL_MS_DEFAULT),
            NumberUtils.toLong(
                cmdLine.getOptionValue("taskQueuedTimeoutMs"),
                OperationTask.QUEUED_TIMEOUT_MS_DEFAULT),
            NumberUtils.toLong(
                cmdLine.getOptionValue("taskTimeoutMs"), OperationTask.TASK_TIMEOUT_MS_DEFAULT));
    ThreadPoolExecutor jobExecutors = null;
    ThreadPoolExecutor statusExecutors = null;
    if (isMultiOperationMode(cmdLine)) {
      jobExecutors =
          new ThreadPoolExecutor(
              getNumJobsSubmitter(cmdLine),
              getNumJobsSubmitter(cmdLine),
              0L,
              TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<Runnable>());
      statusExecutors =
          new ThreadPoolExecutor(
              getNumJobsPoller(cmdLine),
              getNumJobsPoller(cmdLine),
              0L,
              TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<Runnable>());
    } else {
      jobExecutors =
          new ThreadPoolExecutor(
              getNumParallelJobs(cmdLine),
              getNumParallelJobs(cmdLine),
              0L,
              TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<Runnable>());
    }
    JobsScheduler app =
        new JobsScheduler(
            jobExecutors,
            statusExecutors,
            tasksFactory,
            tablesClient,
            jobsClient,
            new OperationTaskManager(operationType),
            new JobInfoManager(operationType));
    OperationTasksBuilder builder =
        new OperationTasksBuilder(
            tasksFactory,
            app.tablesClient,
            getNumParallelMetadataFetch(cmdLine),
            app.operationTaskManager,
            app.jobInfoManager);
    app.run(
        operationType,
        operationTaskCls.toString(),
        properties,
        builder,
        isDryRun(cmdLine),
        getTasksWaitHours(cmdLine),
        isParallelMetadataFetchModeEnabled(cmdLine),
        getNumParallelMetadataFetch(cmdLine),
        isMultiOperationMode(cmdLine),
        getConcurrentJobsLimit(cmdLine),
        getNumJobsSubmitter(cmdLine),
        getJobSubmissionPauseInMillis(cmdLine),
        getSubmitOperationPreSlaGracePeriodInMinutes(cmdLine),
        getStatusOperationPreSlaGracePeriodInMinutes(cmdLine));
  }

  protected void run(
      JobConf.JobTypeEnum jobType,
      String taskType,
      Properties properties,
      OperationTasksBuilder builder,
      boolean isDryRun,
      int tasksWaitHours,
      boolean parallelMetadataFetchMode,
      int numParallelMetadataFetch,
      boolean isMultiOperationMode,
      int concurrentJobsLimit,
      int jobsSubmissionBatchSize,
      int jobSubmissionPauseInMillis,
      int submitOperationPreSlaGracePeriodInMinutes,
      int statusOperationPreSlaGracePeriodInMinutes) {
    this.builder = builder;
    this.run(
        jobType,
        taskType,
        properties,
        isDryRun,
        tasksWaitHours,
        parallelMetadataFetchMode,
        numParallelMetadataFetch,
        isMultiOperationMode,
        concurrentJobsLimit,
        jobsSubmissionBatchSize,
        jobSubmissionPauseInMillis,
        submitOperationPreSlaGracePeriodInMinutes,
        statusOperationPreSlaGracePeriodInMinutes);
  }

  protected void run(
      JobConf.JobTypeEnum jobType,
      String taskType,
      Properties properties,
      boolean isDryRun,
      int tasksWaitHours,
      boolean parallelMetadataFetchMode,
      int numParallelMetadataFetch,
      boolean isMultiOperationMode,
      int concurrentJobsLimit,
      int jobsSubmissionBatchSize,
      int jobSubmissionPauseInMillis,
      int submitOperationPreSlaGracePeriodInMinutes,
      int statusOperationPreSlaGracePeriodInMinutes) {
    long startTimeMillis = System.currentTimeMillis();
    METER.counterBuilder("scheduler_start_count").build().add(1);
    jobStateCountMap = new ConcurrentHashMap<>();
    Arrays.stream(JobState.values()).sequential().forEach(s -> jobStateCountMap.put(s, 0));
    if (builder == null) {
      builder =
          new OperationTasksBuilder(
              taskFactory,
              tablesClient,
              numParallelMetadataFetch,
              operationTaskManager,
              jobInfoManager);
    }
    log.info("Fetching task list based on the job type: {}", jobType);
    List<OperationTask<?>> taskList = new ArrayList<>();
    List<OperationTask<?>> statusTaskList = new ArrayList<>();
    List<Future<Optional<JobState>>> taskFutures = new ArrayList<>();
    List<Future<Optional<JobState>>> statusTaskFutures = new ArrayList<>();
    // Sets the default operation mode as existing SINGLE
    OperationMode operationMode = OperationMode.SINGLE;
    // If Parallel metadata fetch mode and multi operation mode both are enable table metadata is
    // fetched in parallel
    // and jobs submission works in SUBMIT and POLL mode.
    if (parallelMetadataFetchMode && isMultiOperationMode) {
      operationMode = OperationMode.SUBMIT;
      // table metadata is fetched in parallel and add to a queue. On the other, hand
      // metadata is read from queue and jobs are submitted in parallel as they arrive.
      // Asynchronously fetches operation tasks and adds them to a queue
      builder.buildOperationTaskListInParallel(jobType, operationMode);
      log.info("Submitting and running jobs for job type: {}", jobType);
      // Asynchronously submit jobs
      submitLaunchJobTasks(
          jobType,
          jobExecutors,
          concurrentJobsLimit,
          jobsSubmissionBatchSize,
          jobSubmissionPauseInMillis,
          startTimeMillis,
          tasksWaitHours,
          submitOperationPreSlaGracePeriodInMinutes,
          taskList,
          taskFutures);
      // Submit status check jobs
      CompletableFuture<Void> completableFuture =
          submitJobStatusTasks(
              jobType,
              statusExecutors,
              startTimeMillis,
              tasksWaitHours,
              statusOperationPreSlaGracePeriodInMinutes,
              statusTaskList,
              statusTaskFutures);
      // Wait on the main thread for status check job completion
      completableFuture.join();
    } else if (parallelMetadataFetchMode) {
      // Table metadata is fetched in parallel and job submission works in SINGLE mode
      builder.buildOperationTaskListInParallel(jobType, operationMode);
      submitJobs(
          jobType,
          jobExecutors,
          startTimeMillis,
          tasksWaitHours,
          submitOperationPreSlaGracePeriodInMinutes,
          taskList,
          taskFutures);
      // get job status from task future and update job state for SINGLE mode
      updateJobStateFromTaskFutures(
          jobType, jobExecutors, taskList, taskFutures, startTimeMillis, tasksWaitHours, false);
    } else {
      // Sequential metadata fetch continues to work with existing SINGLE operation mode.
      // all the table metadata are fetched first and then jobs are submitted.
      taskList = builder.buildOperationTaskList(jobType, properties, METER, operationMode);
      if (isDryRun && jobType.equals(JobConf.JobTypeEnum.ORPHAN_DIRECTORY_DELETION)) {
        log.info("Dry running {} jobs based on the job type: {}", taskList.size(), jobType);
        for (OperationTask<?> operationTask : taskList) {
          log.info("Task {}", operationTask);
        }
        return;
      }
      log.info(
          "Submitting and running {} jobs based on the job type: {}", taskList.size(), jobType);

      for (OperationTask<?> operationTask : taskList) {
        taskFutures.add(jobExecutors.submit(operationTask));
      }
      // get job status from task future and update job state for SINGLE mode
      updateJobStateFromTaskFutures(
          jobType, jobExecutors, taskList, taskFutures, startTimeMillis, tasksWaitHours, false);
    }

    log.info(
        "Finishing scheduler for job type {}, tasks stats: {} created, {} succeeded, {} running, {} queued"
            + " {} cancelled (scheduler timeout), {} failed, {} skipped (no state)",
        jobType,
        taskList.size(),
        jobStateCountMap.get(JobState.SUCCEEDED),
        jobStateCountMap.get(JobState.RUNNING),
        jobStateCountMap.get(JobState.QUEUED),
        jobStateCountMap.get(JobState.CANCELLED),
        jobStateCountMap.get(JobState.FAILED),
        jobStateCountMap.get(JobState.SKIPPED));
    long totalRunDuration =
        TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis() - startTimeMillis);
    log.info(
        "The total run duration of job scheduler for job type {} is : {} hours",
        jobType,
        totalRunDuration);
    jobExecutors.shutdown();
    if (statusExecutors != null) {
      statusExecutors.shutdown();
    }
    METER.counterBuilder("scheduler_end_count").build().add(1);
    reportMetrics(jobStateCountMap, taskType, startTimeMillis);
  }

  void reportMetrics(
      Map<JobState, Integer> jobStateCountMap, String taskType, long startTimeMillis) {
    LongCounter successfulJobCounter =
        METER.counterBuilder(AppConstants.SUCCESSFUL_JOB_COUNT).build();
    LongCounter failedJobCounter = METER.counterBuilder(AppConstants.FAILED_JOB_COUNT).build();
    LongCounter cancelledJobCounter =
        METER.counterBuilder(AppConstants.CANCELLED_JOB_COUNT).build();
    LongCounter runningJobCounter = METER.counterBuilder(AppConstants.RUNNING_JOB_COUNT).build();
    LongCounter queuedJobCounter = METER.counterBuilder(AppConstants.QUEUED_JOB_COUNT).build();
    Attributes attributes = Attributes.of(AttributeKey.stringKey(AppConstants.TYPE), taskType);
    successfulJobCounter.add(jobStateCountMap.get(JobState.SUCCEEDED), attributes);
    failedJobCounter.add(jobStateCountMap.get(JobState.FAILED), attributes);
    cancelledJobCounter.add(jobStateCountMap.get(JobState.CANCELLED), attributes);
    runningJobCounter.add(jobStateCountMap.get(JobState.RUNNING), attributes);
    queuedJobCounter.add(jobStateCountMap.get(JobState.QUEUED), attributes);
    METER
        .gaugeBuilder(AppConstants.RUN_DURATION_SCHEDULER)
        .ofLongs()
        .setUnit(TimeUnit.MILLISECONDS.name())
        .buildWithCallback(
            measurement -> {
              measurement.record(System.currentTimeMillis() - startTimeMillis, attributes);
            });
    // TODO: remove METER with histogram after all jobs dashboards, alerts have been migrated to use
    // gauge
    METER
        .histogramBuilder("scheduler_run_duration")
        .ofLongs()
        .setUnit(TimeUnit.MILLISECONDS.name())
        .build()
        .record(System.currentTimeMillis() - startTimeMillis, attributes);
  }

  protected static CommandLine parseArgs(String[] args) {
    Options options = new Options();
    options.addOption(
        Option.builder(null)
            .required()
            .hasArg()
            .longOpt("tablesURL")
            .desc("Tables endpoint URL")
            .build());
    options.addOption(
        Option.builder(null)
            .required()
            .hasArg()
            .longOpt("type")
            .desc(String.format("Scheduler job type: %s", SUPPORTED_OPERATIONS_STRING))
            .build());
    options.addOption(
        Option.builder(null).required().hasArg().longOpt("cluster").desc("Cluster id").build());
    options.addOption(
        Option.builder(null)
            .required()
            .hasArg()
            .longOpt("jobsURL")
            .desc("Jobs endpoint URL")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("numParallelJobs")
            .desc("Number of jobs to run in parallel")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("tokenFile")
            .desc("File containing token to authenticate with /tables service")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("databaseFilter")
            .desc("Regexp for filtering databases, defaults to .*")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("tableFilter")
            .desc("Regexp for filtering tables, defaults to .*")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("tableMinAgeThresholdHours")
            .desc("Time in hour for filtering older tables, defaults to 72")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg(false)
            .longOpt("dryRun")
            .desc("Dry run without actual action")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("tasksWaitHours")
            .desc("Timeout in hours for scheduler")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("taskPollIntervalMs")
            .desc("Poll interval in milliseconds for an individual task")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("taskQueuedTimeoutMs")
            .desc("Timeout in milliseconds for an individual task in queued state")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("taskTimeoutMs")
            .desc("Timeout in milliseconds for an individual task to poll jobs status")
            .build());
    // TODO: move these to ODD specific config
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("storageType")
            .desc("Storage type to fetch file system")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("storageUri")
            .desc("Storage uri to fetch file system")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("rootPath")
            .desc("Root path of the file system")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt(OperationTasksBuilder.MAX_STRATEGIES_COUNT)
            .desc("Maximum number of strategies to schedule")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt(OperationTasksBuilder.MAX_COST_BUDGET_GB_HRS)
            .desc("Maximum compute cost budget in GB hours")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("parallelMetadataFetchMode")
            .desc("Turn on/off parallel metadata fetch mode")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("numParallelMetadataFetch")
            .desc("Number of threads to run in parallel for metadata fetch")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("multiOperationMode")
            .desc(
                "Is job submission and status polling done in different mode for maximum throughput")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("numJobsSubmitter")
            .desc("Number of threads to run in parallel for jobs submission")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("numJobsPoller")
            .desc("Number of threads to run in parallel for polling jobs status")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("jobSubmissionPauseInMillis")
            .desc("Pause in milliseconds between submission batches")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("queuePollTimeoutInMinutes")
            .desc("Queue poll timeout in minutes")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("submitOperationPreSlaGracePeriodInMinutes")
            .desc("Pre sla grace period in minutes to stop submitting new jobs")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg()
            .longOpt("statusOperationPreSlaGracePeriodInMinutes")
            .desc("Pre sla grace period in minutes to stop submitting new status check jobs")
            .build());
    CommandLineParser parser = new BasicParser();
    try {
      return parser.parse(options, args);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  protected static Class<? extends OperationTask> getOperationTaskCls(String operationType) {
    if (!OPERATIONS_REGISTRY.containsKey(operationType)) {
      throw new RuntimeException(
          String.format(
              "Unsupported job type %s, expected one of %s",
              operationType, SUPPORTED_OPERATIONS_STRING));
    }
    return OPERATIONS_REGISTRY.get(operationType);
  }

  protected static JobConf.JobTypeEnum getOperationJobType(CommandLine cmdLine) {
    String operationType = cmdLine.getOptionValue("type");
    return JobConf.JobTypeEnum.fromValue(operationType);
  }

  protected static boolean isDryRun(CommandLine cmdLine) {
    return cmdLine.hasOption("dryRun");
  }

  protected static String getToken(CommandLine cmdLine) {
    String tokenFilename = cmdLine.getOptionValue("tokenFile");
    if (tokenFilename == null) {
      return null;
    }
    Path path = Paths.get(tokenFilename);
    try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      return br.readLine();
    } catch (IOException e) {
      throw new RuntimeException(String.format("Could not read token file %s", tokenFilename), e);
    }
  }

  protected static TablesClientFactory getTablesClientFactory(CommandLine cmdLine) {
    String token = getToken(cmdLine);
    int tableMinAgeThresholdHours =
        NumberUtils.toInt(
            cmdLine.getOptionValue("tableMinAgeThresholdHours"),
            TABLE_MIN_AGE_THRESHOLD_HOURS_DEFAULT);
    DatabaseTableFilter filter =
        DatabaseTableFilter.of(
            cmdLine.getOptionValue("databaseFilter", ".*"),
            cmdLine.getOptionValue("tableFilter", ".*"),
            tableMinAgeThresholdHours);
    ParameterizedHdfsStorageProvider hdfsStorageProvider =
        ParameterizedHdfsStorageProvider.of(
            cmdLine.getOptionValue("storageType", null),
            cmdLine.getOptionValue("storageUri", null),
            cmdLine.getOptionValue("rootPath", null));

    return new TablesClientFactory(
        cmdLine.getOptionValue("tablesURL"), filter, token, hdfsStorageProvider);
  }

  protected static JobsClientFactory getJobsClientFactory(CommandLine cmdLine) {
    return new JobsClientFactory(
        cmdLine.getOptionValue("jobsURL"), cmdLine.getOptionValue("cluster"));
  }

  protected static int getNumParallelJobs(CommandLine cmdLine) {
    int ret = MAX_NUM_CONCURRENT_JOBS_DEFAULT;
    if (cmdLine.hasOption("numParallelJobs")) {
      ret = Integer.parseInt(cmdLine.getOptionValue("numParallelJobs"));
    }
    return ret;
  }

  protected static int getTasksWaitHours(CommandLine cmdLine) {
    return NumberUtils.toInt(cmdLine.getOptionValue("tasksWaitHours"), TASKS_WAIT_HOURS_DEFAULT);
  }

  protected static Properties getAdditionalProperties(CommandLine cmdLine) {
    Properties result = new Properties();
    if (cmdLine.hasOption(OperationTasksBuilder.MAX_COST_BUDGET_GB_HRS)) {
      result.setProperty(
          OperationTasksBuilder.MAX_COST_BUDGET_GB_HRS,
          cmdLine.getOptionValue(OperationTasksBuilder.MAX_COST_BUDGET_GB_HRS));
    }
    if (cmdLine.hasOption(OperationTasksBuilder.MAX_STRATEGIES_COUNT)) {
      result.setProperty(
          OperationTasksBuilder.MAX_STRATEGIES_COUNT,
          cmdLine.getOptionValue(OperationTasksBuilder.MAX_STRATEGIES_COUNT));
    }
    return result;
  }

  /**
   * Submits tasks to launch jobs. Polls from the operation tasks queue and submits jobs. On jobs
   * submit completion collects status on the futures.
   *
   * @param jobType
   * @param jobExecutors
   * @param concurrentJobsLimit
   * @param jobsSubmissionBatchSize
   * @param jobSubmissionPauseInMillis
   * @param startTimeMillis
   * @param tasksWaitHours
   * @param submitOperationPreSlaGracePeriodInMinutes
   * @param taskList
   * @param taskFutures
   */
  private void submitLaunchJobTasks(
      JobConf.JobTypeEnum jobType,
      ThreadPoolExecutor jobExecutors,
      int concurrentJobsLimit,
      int jobsSubmissionBatchSize,
      int jobSubmissionPauseInMillis,
      long startTimeMillis,
      int tasksWaitHours,
      int submitOperationPreSlaGracePeriodInMinutes,
      List<OperationTask<?>> taskList,
      List<Future<Optional<JobState>>> taskFutures) {
    // fetch operation tasks from queue and submits jobs until terminate signal and queue is empty
    CompletableFuture.runAsync(
            () -> {
              int currentBatchSize = 0;
              do {
                try {
                  OperationTask<?> task = operationTaskManager.getData();
                  if (task != null) {
                    log.debug("Submitting job launch task {}", task);
                    taskList.add(task);
                    taskFutures.add(jobExecutors.submit(task));
                    ++currentBatchSize;
                  }
                  long passedTimeMillis = System.currentTimeMillis() - startTimeMillis;
                  long remainingTimeMillis =
                      (TimeUnit.HOURS.toMillis(tasksWaitHours)
                              - TimeUnit.MINUTES.toMillis(
                                  submitOperationPreSlaGracePeriodInMinutes))
                          - passedTimeMillis;
                  if (remainingTimeMillis <= 0) {
                    if (!operationTaskManager.isEmpty()) {
                      log.info(
                          "Reached pre SLA grace period while submitting job launch tasks. Total time elapsed: {} hrs for job type: {}",
                          TimeUnit.MILLISECONDS.toHours(passedTimeMillis),
                          jobType);
                      log.info(
                          "The remaining jobs launch tasks: {} could not be submitted due to pre SLA signal for job type: {}",
                          operationTaskManager.getCurrentDataCount(),
                          jobType);
                      operationTaskManager.clear();
                      break;
                    }
                  }
                  if (jobInfoManager.runningJobsCount() >= concurrentJobsLimit
                      || currentBatchSize >= jobsSubmissionBatchSize) {
                    // Delay between job submission batches
                    Thread.sleep(jobSubmissionPauseInMillis);
                    currentBatchSize = 0;
                  }
                } catch (InterruptedException e) {
                  log.warn(
                      "Interrupted exception while polling from the operation task queue for job type: {}",
                      jobType);
                }
              } while (operationTaskManager.hasNext());
            })
        .exceptionally(
            ex -> {
              log.info(
                  "Exception occurred while submitting jobs for job type: {}, exception: ",
                  jobType,
                  ex);
              return null; // recover from the exception (if there is any) with null and the chain
              // completes normally
            })
        .thenRun(
            () -> {
              log.info(
                  "The total jobs submitted: {} for the job type: {}", taskList.size(), jobType);
              // get job status from task future and update job state for SUBMIT mode
              updateJobStateFromTaskFutures(
                  jobType,
                  jobExecutors,
                  taskList,
                  taskFutures,
                  startTimeMillis,
                  tasksWaitHours,
                  true);
              // Marked as completed after submission status check to ensure that all the submitted
              // jobs are added to
              // the submitted queue
              jobInfoManager.updateDataGenerationCompletion();
            });
  }

  /**
   * Submits status jobs to check the status of the launched jobs. Polls the submitted jobs from the
   * submit queue and submits status checks jobs. On status jobs submit completion collects status
   * on the futures.
   *
   * @param jobType
   * @param statusExecutors
   * @param startTimeMillis
   * @param tasksWaitHours
   * @param statusOperationPreSlaGracePeriodInMinutes
   * @param statusTaskList
   * @param statusTaskFutures
   * @return CompletableFuture
   */
  private CompletableFuture<Void> submitJobStatusTasks(
      JobConf.JobTypeEnum jobType,
      ThreadPoolExecutor statusExecutors,
      long startTimeMillis,
      int tasksWaitHours,
      int statusOperationPreSlaGracePeriodInMinutes,
      List<OperationTask<?>> statusTaskList,
      List<Future<Optional<JobState>>> statusTaskFutures) {
    // fetch submitted jobs from queue and submits status check jobs until terminate signal and
    // queue is empty
    return CompletableFuture.runAsync(
            () -> {
              do {
                try {
                  JobInfo jobInfo = jobInfoManager.getData();
                  log.debug("Received job info: {} from submitted job queue", jobInfo);
                  if (jobInfo != null) {
                    Optional<OperationTask<?>> optionalOperationTask =
                        builder.createStatusOperationTask(
                            jobType, jobInfo.getMetadata(), jobInfo.getJobId(), OperationMode.POLL);
                    if (optionalOperationTask.isPresent()) {
                      log.info("Submitting status task {}", optionalOperationTask.get());
                      statusTaskList.add(optionalOperationTask.get());
                      Future<Optional<JobState>> futureTask =
                          statusExecutors.submit(optionalOperationTask.get());
                      statusTaskFutures.add(futureTask);
                    }
                  }
                  long passedTimeMillis = System.currentTimeMillis() - startTimeMillis;
                  long remainingTimeMillis =
                      (TimeUnit.HOURS.toMillis(tasksWaitHours)
                              - TimeUnit.MINUTES.toMillis(
                                  statusOperationPreSlaGracePeriodInMinutes))
                          - passedTimeMillis;
                  if (remainingTimeMillis <= 0) {
                    if (!jobInfoManager.isEmpty()) {
                      log.info(
                          "Reached job scheduler execution SLA while submitting job status tasks. Total time elapsed: {} hrs for job type: {}",
                          TimeUnit.MILLISECONDS.toHours(passedTimeMillis),
                          jobType);
                      log.info(
                          "The remaining jobs status tasks: {} could not be submitted due to SLA signal for job type: {}",
                          jobInfoManager.getCurrentDataCount(),
                          jobType);
                      jobInfoManager.clear();
                      break;
                    }
                  }
                } catch (InterruptedException e) {
                  log.warn(
                      "Interrupted exception while polling submitted jobs from the queue: {}",
                      jobType);
                }
              } while (jobInfoManager.hasNext());
            })
        .exceptionally(
            ex -> {
              log.info(
                  "Exception occurred while submitting status tasks for job type: {}, exception: ",
                  jobType,
                  ex);
              return null; // recover from the exception (if there is any) with null and the chain
              // completes normally
            })
        .thenRun(
            () -> {
              jobStatusTasksSubmissionCompleted.set(true);
              log.info("Job status check tasks submission completed for job type: {}", jobType);
              // get job status from task future and update job state for STATUS mode
              updateJobStateFromTaskFutures(
                  jobType,
                  statusExecutors,
                  statusTaskList,
                  statusTaskFutures,
                  startTimeMillis,
                  tasksWaitHours,
                  false);
            });
  }

  private void submitJobs(
      JobConf.JobTypeEnum jobType,
      ThreadPoolExecutor jobExecutors,
      long startTimeMillis,
      int tasksWaitHours,
      int submitOperationPreSlaGracePeriodInMinutes,
      List<OperationTask<?>> taskList,
      List<Future<Optional<JobState>>> taskFutures) {
    do {
      try {
        OperationTask<?> task = operationTaskManager.getData();
        if (task != null) {
          log.debug("Submitting job launch task {}", task);
          taskList.add(task);
          taskFutures.add(jobExecutors.submit(task));
        }
        long passedTimeMillis = System.currentTimeMillis() - startTimeMillis;
        long remainingTimeMillis =
            (TimeUnit.HOURS.toMillis(tasksWaitHours)
                    - TimeUnit.MINUTES.toMillis(submitOperationPreSlaGracePeriodInMinutes))
                - passedTimeMillis;
        if (remainingTimeMillis <= 0) {
          if (!operationTaskManager.isEmpty()) {
            log.info(
                "Reached pre SLA grace period while submitting job launch tasks. Total time elapsed: {} hrs for job type: {}",
                TimeUnit.MILLISECONDS.toHours(passedTimeMillis),
                jobType);
            log.info(
                "The remaining jobs launch tasks: {} could not be submitted due to pre SLA signal for job type: {}",
                operationTaskManager.getCurrentDataCount(),
                jobType);
            operationTaskManager.clear();
            break;
          }
        }
      } catch (InterruptedException e) {
        log.warn(
            "Interrupted exception while polling from the operation task queue for job type: {}",
            jobType);
      }
    } while (operationTaskManager.hasNext());
    log.info(
        "The total operation tasks submitted: {} for the job type: {}", taskList.size(), jobType);
  }

  protected static int getNumParallelMetadataFetch(CommandLine cmdLine) {
    int ret = MAX_NUM_CONCURRENT_METADATA_FETCH_THREAD_DEFAULT;
    if (cmdLine.hasOption("numParallelMetadataFetch")) {
      ret = Integer.parseInt(cmdLine.getOptionValue("numParallelMetadataFetch"));
    }
    return ret;
  }

  protected static boolean isParallelMetadataFetchModeEnabled(CommandLine cmdLine) {
    if (cmdLine.hasOption("parallelMetadataFetchMode")) {
      return Boolean.parseBoolean(cmdLine.getOptionValue("parallelMetadataFetchMode"));
    }
    return PARALLEL_METADATA_FETCH_MODE_DEFAULT;
  }

  protected static boolean isMultiOperationMode(CommandLine cmdLine) {
    if (cmdLine.hasOption("multiOperationMode")) {
      return Boolean.parseBoolean(cmdLine.getOptionValue("multiOperationMode"));
    }
    return false;
  }

  protected static int getNumJobsSubmitter(CommandLine cmdLine) {
    if (cmdLine.hasOption("numJobsSubmitter")) {
      return Integer.parseInt(cmdLine.getOptionValue("numJobsSubmitter"));
    }
    return MAX_JOBS_SUBMITTER_DEFAULT;
  }

  protected static int getNumJobsPoller(CommandLine cmdLine) {
    if (cmdLine.hasOption("numJobsPoller")) {
      return Integer.parseInt(cmdLine.getOptionValue("numJobsPoller"));
    }
    return MAX_JOBS_POLLER_DEFAULT;
  }

  protected static int getConcurrentJobsLimit(CommandLine cmdLine) {
    if (cmdLine.hasOption("concurrentJobsLimit")) {
      return Integer.parseInt(cmdLine.getOptionValue("concurrentJobsLimit"));
    }
    return MAX_NUM_CONCURRENT_JOBS_DEFAULT;
  }

  protected static int getJobSubmissionPauseInMillis(CommandLine cmdLine) {
    if (cmdLine.hasOption("jobSubmissionPauseInMillis")) {
      return Integer.parseInt(cmdLine.getOptionValue("jobSubmissionPauseInMillis"));
    }
    return JOB_SUBMISSION_PAUSE_IN_MILLIS_DEFAULT;
  }

  protected static int getQueuePollTimeoutInMinutes(CommandLine cmdLine) {
    if (cmdLine.hasOption("queuePollTimeoutInMinutes")) {
      return Integer.parseInt(cmdLine.getOptionValue("queuePollTimeoutInMinutes"));
    }
    return QUEUE_POLL_TIMEOUT_MINUTES_DEFAULT;
  }

  protected static int getSubmitOperationPreSlaGracePeriodInMinutes(CommandLine cmdLine) {
    if (cmdLine.hasOption("submitOperationPreSlaGracePeriodInMinutes")) {
      return Integer.parseInt(cmdLine.getOptionValue("submitOperationPreSlaGracePeriodInMinutes"));
    }
    return SUBMIT_OPERATION_PRE_SLA_GRACE_PERIOD_MINUTES_DEFAULT;
  }

  protected static int getStatusOperationPreSlaGracePeriodInMinutes(CommandLine cmdLine) {
    if (cmdLine.hasOption("statusOperationPreSlaGracePeriodInMinutes")) {
      return Integer.parseInt(cmdLine.getOptionValue("statusOperationPreSlaGracePeriodInMinutes"));
    }
    return STATUS_OPERATION_PRE_SLA_GRACE_PERIOD_MINUTES_DEFAULT;
  }

  private void updateJobStateFromTaskFutures(
      JobConf.JobTypeEnum jobType,
      ThreadPoolExecutor executors,
      List<OperationTask<?>> taskList,
      List<Future<Optional<JobState>>> taskFutures,
      long startTimeMillis,
      int tasksWaitHours,
      boolean skipStateCountUpdate) {
    for (int taskIndex = 0; taskIndex < taskList.size(); ++taskIndex) {
      Optional<JobState> jobState = Optional.empty();
      OperationTask<?> task = taskList.get(taskIndex);
      Future<Optional<JobState>> taskFuture = taskFutures.get(taskIndex);
      try {
        long passedTimeMillis = System.currentTimeMillis() - startTimeMillis;
        long remainingTimeMillis = TimeUnit.HOURS.toMillis(tasksWaitHours) - passedTimeMillis;
        log.info("Task {} has remainingTimeMillis={}", task.getJobId(), remainingTimeMillis);
        jobState = taskFuture.get(remainingTimeMillis, TimeUnit.MILLISECONDS);
        log.info(
            "Successfully get job state for task {}: {}",
            task.getJobId(),
            jobState.orElse(JobState.SKIPPED));
      } catch (ExecutionException e) {
        log.error(String.format("Operation for %s failed with exception", task), e);
        jobStateCountMap.put(JobState.FAILED, jobStateCountMap.get(JobState.FAILED) + 1);
      } catch (InterruptedException e) {
        throw new RuntimeException("Scheduler thread is interrupted, shutting down", e);
      } catch (TimeoutException e) {
        // Clear queue to stop internal tasks submission
        if (!executors.getQueue().isEmpty()) {
          log.warn(
              "Drops {} tasks for job type {} from wait queue due to timeout",
              executors.getQueue().size(),
              jobType);
          executors.getQueue().clear();
        }
        log.warn(
            "Attempting to cancel task for {} because of timeout of {} hours",
            task,
            tasksWaitHours);
        if (taskFuture.cancel(true)) {
          log.warn("Cancelled task for {} because of timeout of {} hours", task, tasksWaitHours);
        }
      } finally {
        if (!skipStateCountUpdate) {
          if (jobState.isPresent()) {
            jobStateCountMap.put(jobState.get(), jobStateCountMap.get(jobState.get()) + 1);
          } else if (taskFuture.isCancelled()) {
            // Even though the jobs are reported as cancelled, they might be queued or running.
            jobStateCountMap.put(JobState.CANCELLED, jobStateCountMap.get(JobState.CANCELLED) + 1);
          } else {
            // Jobs that are skipped due to replica or missing retention policy, etc.
            jobStateCountMap.put(JobState.SKIPPED, jobStateCountMap.get(JobState.SKIPPED) + 1);
          }
        }
      }
    }
    log.info("Completed collecting jobs state on futures for job type {}", jobType);
  }
}
