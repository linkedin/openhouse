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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
  private static final int TASKS_WAIT_TIMEOUT_HOURS = 12;
  private static final int DEFAULT_MAX_NUM_CONCURRENT_JOBS = 40;
  private static final int DEFAULT_TABLE_CREATION_CUTOFF_HOUR = 72;
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

  private final ExecutorService executorService;
  private final OperationTaskFactory<? extends OperationTask> taskFactory;
  private final TablesClient tablesClient;

  public JobsScheduler(
      ExecutorService executorService,
      OperationTaskFactory<? extends OperationTask> taskFactory,
      TablesClient tablesClient) {
    this.executorService = executorService;
    this.taskFactory = taskFactory;
    this.tablesClient = tablesClient;
  }

  public static void main(String[] args) {
    log.info("Starting scheduler");
    CommandLine cmdLine = parseArgs(args);
    JobConf.JobTypeEnum operationType = getOperationJobType(cmdLine);
    Class<? extends OperationTask> operationTaskCls = getOperationTaskCls(operationType.toString());
    TablesClientFactory tablesClientFactory = getTablesClientFactory(cmdLine);
    OperationTaskFactory<? extends OperationTask> tasksFactory =
        new OperationTaskFactory<>(
            operationTaskCls, getJobsClientFactory(cmdLine), tablesClientFactory);
    JobsScheduler app =
        new JobsScheduler(
            Executors.newFixedThreadPool(getNumParallelJobs(cmdLine)),
            tasksFactory,
            tablesClientFactory.create());
    app.run(operationType, operationTaskCls.toString(), isDryRun(cmdLine));
  }

  protected void run(JobConf.JobTypeEnum jobType, String taskType, boolean isDryRun) {
    long startTimeMillis = System.currentTimeMillis();
    METER.counterBuilder("scheduler_start_count").build().add(1);
    Map<JobState, Integer> jobStateCountMap = new HashMap<>();
    Arrays.stream(JobState.values()).sequential().forEach(s -> jobStateCountMap.put(s, 0));

    log.info("Fetching task list based on the job type: {}", jobType);
    List<OperationTask> taskList =
        new OperationTasksBuilder(taskFactory, tablesClient).buildOperationTaskList(jobType);
    if (isDryRun && jobType.equals(JobConf.JobTypeEnum.ORPHAN_DIRECTORY_DELETION)) {
      log.info("Dry running {} jobs based on the job type: {}", taskList.size(), jobType);
      for (int taskIndex = 0; taskIndex < taskList.size(); ++taskIndex) {
        log.info("metadata {}", taskList.get(taskIndex).getMetadata());
      }
      return;
    }
    log.info("Submitting and running {} jobs based on the job type: {}", taskList.size(), jobType);
    List<Future<Optional<JobState>>> taskFutures = new ArrayList<>();
    for (int taskIndex = 0; taskIndex < taskList.size(); ++taskIndex) {
      taskFutures.add(executorService.submit(taskList.get(taskIndex)));
    }

    int emptyStateJobCount = 0;
    for (int taskIndex = 0; taskIndex < taskList.size(); ++taskIndex) {
      Optional<JobState> jobState = Optional.empty();
      OperationTask task = taskList.get(taskIndex);
      Future<Optional<JobState>> taskFuture = taskFutures.get(taskIndex);
      try {
        long passedTimeMillis = System.currentTimeMillis() - startTimeMillis;
        long remainingTimeMillis =
            TimeUnit.HOURS.toMillis(TASKS_WAIT_TIMEOUT_HOURS) - passedTimeMillis;
        if (remainingTimeMillis <= 0) {
          // treat as a global timeout case similar to future.get timeout
          throw new TimeoutException();
        }
        jobState = taskFuture.get(remainingTimeMillis, TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        log.error(String.format("Operation for %s failed with exception", task.getMetadata()), e);
        jobStateCountMap.put(JobState.FAILED, jobStateCountMap.get(JobState.FAILED) + 1);
      } catch (InterruptedException e) {
        throw new RuntimeException("Scheduler thread is interrupted, shutting down", e);
      } catch (TimeoutException e) {
        if (!taskFuture.isDone()) {
          log.warn(
              "Cancelling job for {} because of timeout of {} hours",
              task.getMetadata(),
              TASKS_WAIT_TIMEOUT_HOURS);
          taskFuture.cancel(true);
          jobStateCountMap.put(JobState.CANCELLED, jobStateCountMap.get(JobState.CANCELLED) + 1);
        }
      } finally {
        if (jobState.isPresent()) {
          jobStateCountMap.put(jobState.get(), jobStateCountMap.get(jobState.get()) + 1);
        } else {
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
    executorService.shutdown();
    METER.counterBuilder("scheduler_end_count").build().add(1);
    reportSchedulerMetrics(jobStateCountMap, taskType, startTimeMillis);
  }

  void reportSchedulerMetrics(
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
            .longOpt("timeFilter")
            .desc("Time in hour for filtering older tables, defaults to 72")
            .build());
    options.addOption(
        Option.builder(null)
            .required(false)
            .hasArg(false)
            .longOpt("dryRun")
            .desc("Dry run without actual action")
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
    DatabaseTableFilter filter =
        DatabaseTableFilter.of(
            cmdLine.getOptionValue("databaseFilter", ".*"),
            cmdLine.getOptionValue("tableFilter", ".*"));
    ParameterizedHdfsStorageProvider hdfsStorageProvider =
        ParameterizedHdfsStorageProvider.of(
            cmdLine.getOptionValue("storageType", null),
            cmdLine.getOptionValue("storageUri", null),
            cmdLine.getOptionValue("rootPath", null));
    int cutoffHours =
        Integer.parseInt(
            cmdLine.getOptionValue(
                "cutoffHours", String.valueOf(DEFAULT_TABLE_CREATION_CUTOFF_HOUR)));

    return new TablesClientFactory(
        cmdLine.getOptionValue("tablesURL"), filter, cutoffHours, token, hdfsStorageProvider);
  }

  protected static JobsClientFactory getJobsClientFactory(CommandLine cmdLine) {
    return new JobsClientFactory(
        cmdLine.getOptionValue("jobsURL"), cmdLine.getOptionValue("cluster"));
  }

  protected static int getNumParallelJobs(CommandLine cmdLine) {
    int ret = DEFAULT_MAX_NUM_CONCURRENT_JOBS;
    if (cmdLine.hasOption("numParallelJobs")) {
      ret = Integer.parseInt(cmdLine.getOptionValue("numParallelJobs"));
    }
    return ret;
  }
}
