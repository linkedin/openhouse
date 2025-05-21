package com.linkedin.openhouse.jobs.scheduler;

import com.linkedin.openhouse.cluster.storage.filesystem.ParameterizedHdfsStorageProvider;
import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.jobs.client.JobsClientFactory;
import com.linkedin.openhouse.jobs.client.TablesClient;
import com.linkedin.openhouse.jobs.client.TablesClientFactory;
import com.linkedin.openhouse.jobs.client.model.JobConf;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationTask;
import com.linkedin.openhouse.jobs.scheduler.tasks.OperationTaskFactory;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
  private static final int OPERATION_TASK_QUEUE_POLL_TIMEOUT_MINUTES_DEFAULT = 5;
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

  private final ThreadPoolExecutor executors;
  private final OperationTaskFactory<? extends OperationTask> taskFactory;
  private final TablesClient tablesClient;
  private final BlockingQueue<OperationTask<?>> operationTaskQueue;
  private AtomicBoolean tableMetadataFetchCompleted = new AtomicBoolean(false);
  private AtomicLong operationTaskCount = new AtomicLong(0);

  public JobsScheduler(
      ThreadPoolExecutor executors,
      OperationTaskFactory<? extends OperationTask> taskFactory,
      TablesClient tablesClient,
      BlockingQueue<OperationTask<?>> operationTaskQueue) {
    this.executors = executors;
    this.taskFactory = taskFactory;
    this.tablesClient = tablesClient;
    this.operationTaskQueue = operationTaskQueue;
  }

  public static void main(String[] args) {
    log.info("Starting scheduler");
    CommandLine cmdLine = parseArgs(args);
    JobConf.JobTypeEnum operationType = getOperationJobType(cmdLine);
    Class<? extends OperationTask> operationTaskCls = getOperationTaskCls(operationType.toString());
    TablesClientFactory tablesClientFactory = getTablesClientFactory(cmdLine);
    Properties properties = getAdditionalProperties(cmdLine);
    OperationTaskFactory<? extends OperationTask> tasksFactory =
        new OperationTaskFactory<>(
            operationTaskCls,
            getJobsClientFactory(cmdLine),
            tablesClientFactory,
            NumberUtils.toLong(
                cmdLine.getOptionValue("taskPollIntervalMs"),
                OperationTask.POLL_INTERVAL_MS_DEFAULT),
            NumberUtils.toLong(
                cmdLine.getOptionValue("taskTimeoutMs"), OperationTask.TIMEOUT_MS_DEFAULT));
    ThreadPoolExecutor executors =
        new ThreadPoolExecutor(
            getNumParallelJobs(cmdLine),
            getNumParallelJobs(cmdLine),
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());
    JobsScheduler app =
        new JobsScheduler(
            executors, tasksFactory, tablesClientFactory.create(), new LinkedBlockingQueue<>());
    app.run(
        operationType,
        operationTaskCls.toString(),
        properties,
        isDryRun(cmdLine),
        getTasksWaitHours(cmdLine),
        isParallelMetadataFetchModeEnabled(cmdLine),
        getNumParallelMetadataFetch(cmdLine));
  }

  protected void run(
      JobConf.JobTypeEnum jobType,
      String taskType,
      Properties properties,
      boolean isDryRun,
      int tasksWaitHours,
      boolean parallelMetadataFetchMode,
      int numParallelMetadataFetch) {
    long startTimeMillis = System.currentTimeMillis();
    METER.counterBuilder("scheduler_start_count").build().add(1);
    Map<JobState, Integer> jobStateCountMap = new HashMap<>();
    Arrays.stream(JobState.values()).sequential().forEach(s -> jobStateCountMap.put(s, 0));

    log.info("Fetching task list based on the job type: {}", jobType);
    List<OperationTask<?>> taskList = new ArrayList<>();
    List<Future<Optional<JobState>>> taskFutures = new ArrayList<>();
    // Fetch metadata and submit jobs
    fetchMetadataAndSubmitTasks(
        jobType,
        properties,
        isDryRun,
        parallelMetadataFetchMode,
        numParallelMetadataFetch,
        taskList,
        taskFutures);

    int emptyStateJobCount = 0;
    for (int taskIndex = 0; taskIndex < taskList.size(); ++taskIndex) {
      Optional<JobState> jobState = Optional.empty();
      OperationTask<?> task = taskList.get(taskIndex);
      Future<Optional<JobState>> taskFuture = taskFutures.get(taskIndex);
      try {
        long passedTimeMillis = System.currentTimeMillis() - startTimeMillis;
        long remainingTimeMillis = TimeUnit.HOURS.toMillis(tasksWaitHours) - passedTimeMillis;
        jobState = taskFuture.get(remainingTimeMillis, TimeUnit.MILLISECONDS);
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
            "Attempting to cancel job for {} because of timeout of {} hours", task, tasksWaitHours);
        if (taskFuture.cancel(true)) {
          log.warn("Cancelled job for {} because of timeout of {} hours", task, tasksWaitHours);
        }
      } finally {
        if (jobState.isPresent()) {
          jobStateCountMap.put(jobState.get(), jobStateCountMap.get(jobState.get()) + 1);
        } else if (taskFuture.isCancelled()) {
          jobStateCountMap.put(JobState.CANCELLED, jobStateCountMap.get(JobState.CANCELLED) + 1);
        } else {
          // Jobs that are skipped due to replica or missing retention policy, etc.
          emptyStateJobCount++;
        }
      }
    }
    log.info(
        "Finishing scheduler for job type {}, tasks stats: {} created, {} succeeded,"
            + " {} cancelled (timeout), {} failed, {} skipped (no state)",
        jobType,
        taskList.size(),
        jobStateCountMap.get(JobState.SUCCEEDED),
        jobStateCountMap.get(JobState.CANCELLED),
        jobStateCountMap.get(JobState.FAILED),
        emptyStateJobCount);
    executors.shutdown();
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
    Attributes attributes = Attributes.of(AttributeKey.stringKey(AppConstants.TYPE), taskType);
    successfulJobCounter.add(jobStateCountMap.get(JobState.SUCCEEDED), attributes);
    failedJobCounter.add(jobStateCountMap.get(JobState.FAILED), attributes);
    cancelledJobCounter.add(jobStateCountMap.get(JobState.CANCELLED), attributes);
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
            .longOpt("taskTimeoutMs")
            .desc("Timeout in milliseconds for an individual task")
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
   * Fetches table metadata in parallel or sequential based on parallelMetadataFetchMode flag. For
   * parallel mode table metadata is fetched in parallel and add to a queue. On the other, hand
   * metadata is read from queue and jobs are submitted in parallel as they arrive. For sequential
   * mode all the table metadata are fetched first and then jobs are submitted.
   *
   * @param jobType
   * @param properties
   * @param isDryRun
   * @param parallelMetadataFetchMode
   * @param numParallelMetadataFetch
   * @param taskList
   * @param taskFutures
   */
  private void fetchMetadataAndSubmitTasks(
      JobConf.JobTypeEnum jobType,
      Properties properties,
      boolean isDryRun,
      boolean parallelMetadataFetchMode,
      int numParallelMetadataFetch,
      List<OperationTask<?>> taskList,
      List<Future<Optional<JobState>>> taskFutures) {
    OperationTasksBuilder builder =
        new OperationTasksBuilder(
            taskFactory,
            tablesClient,
            operationTaskQueue,
            numParallelMetadataFetch,
            tableMetadataFetchCompleted,
            operationTaskCount);
    if (parallelMetadataFetchMode) {
      // Asynchronously fetches operation tasks and adds them to a queue
      builder.buildOperationTaskListInParallel(jobType);
      // fetch operation tasks from queue and submits jobs until terminate signal and queue is empty
      do {
        try {
          OperationTask<?> task =
              operationTaskQueue.poll(
                  OPERATION_TASK_QUEUE_POLL_TIMEOUT_MINUTES_DEFAULT, TimeUnit.MINUTES);
          taskList.add(task);
          taskFutures.add(executors.submit(task));
        } catch (InterruptedException e) {
          log.warn("Interrupted exception while polling from the queue: {}", jobType);
        }
      } while (!(tableMetadataFetchCompleted.get() && operationTaskQueue.isEmpty()));
      log.info("The metadata fetched count: {} for the job type: {}", operationTaskCount, jobType);
      log.info(
          "The total operation tasks submitted: {} for the job type: {}", taskList.size(), jobType);
    } else {
      taskList = builder.buildOperationTaskList(jobType, properties, METER);
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
        taskFutures.add(executors.submit(operationTask));
      }
    }
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
}
