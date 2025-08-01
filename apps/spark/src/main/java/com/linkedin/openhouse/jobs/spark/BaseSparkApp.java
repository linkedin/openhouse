package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.client.ssl.HousetablesApiClientFactory;
import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.common.OtelEmitter;
import com.linkedin.openhouse.housetables.client.api.JobApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.jobs.util.RetryUtil;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.net.MalformedURLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.SparkSession;

/**
 * Base app class implemented common operations, e.g. heartbeats, state update, Spark session
 * creation, args parsing.
 */
@Slf4j
public abstract class BaseSparkApp {
  private static final long HEARTBEAT_INTERVAL_SECONDS_DEFAULT = 300;
  protected static final String METRICS_SCOPE = BaseSparkApp.class.getName();
  protected final String jobId;
  protected final StateManager stateManager;
  private final long heartbeatIntervalSeconds;
  protected final OtelEmitter otelEmitter;
  private final ScheduledExecutorService scheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor();

  protected BaseSparkApp(String jobId, StateManager stateManager) {
    this(jobId, stateManager, HEARTBEAT_INTERVAL_SECONDS_DEFAULT, AppsOtelEmitter.getOtelEmitter());
  }

  protected BaseSparkApp(String jobId, StateManager stateManager, long heartbeatIntervalSeconds) {
    this(jobId, stateManager, heartbeatIntervalSeconds, AppsOtelEmitter.getOtelEmitter());
  }

  protected BaseSparkApp(
      String jobId,
      StateManager stateManager,
      long heartbeatIntervalSeconds,
      OtelEmitter otelEmitter) {
    this.jobId = jobId;
    this.stateManager = stateManager;
    this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    this.otelEmitter = otelEmitter;
  }

  public void run() {
    log.info("Running");
    String className = this.getClass().getSimpleName();
    boolean isSuccess = true;
    try (Operations ops =
        Operations.withCatalog(SparkSession.builder().appName(className).getOrCreate(), null)) {
      log.info("Session created");
      onStarted();
      runInner(ops);
      runValidations(ops);
    } catch (Throwable e) {
      log.error("Run failed, reason: {}", e.getMessage(), e);
      isSuccess = false;
    } finally {
      onFinished(isSuccess);
    }
  }

  /**
   * runValidation performs post job validations. Override it to implement job specific validations
   */
  protected void runValidations(Operations ops) {
    log.info("Default validation for spark app {}", this.getClass().getSimpleName());
  }

  protected abstract void runInner(Operations ops) throws Exception;

  protected static CommandLine createCommandLine(String[] args, List<Option> extraOptions) {
    Options options = new Options();
    options.addOption(new Option("i", "jobId", true, "Job id"));
    options.addOption(new Option("d", "storageURL", true, "HTS endpoint URL"));
    for (Option option : extraOptions) {
      options.addOption(option);
    }
    CommandLineParser parser = new BasicParser();
    try {
      return parser.parse(options, args);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  protected static StateManager createStateManager(CommandLine cmdLine) {
    return new StateManager(
        RetryUtil.getJobsStateApiRetryTemplate(),
        createJobApiClient(cmdLine.getOptionValue("storageURL")));
  }

  protected static String getJobId(CommandLine cmdLine) {
    return cmdLine.getOptionValue("jobId");
  }

  private static JobApi createJobApiClient(String basePath) {
    ApiClient client = null;
    try {
      client = HousetablesApiClientFactory.getInstance().createApiClient(basePath, null, null);
    } catch (MalformedURLException | SSLException e) {
      log.error("Jobs Api client creation failed: Failure while initializing ApiClient", e);
      otelEmitter.count(
          METRICS_SCOPE,
          AppConstants.JOBS_CLIENT_INITIALIZATION_ERROR,
          1,
          Attributes.of(
              AttributeKey.stringKey(AppConstants.SERVICE_NAME), AppConstants.SERVICE_HOUSETABLES));
      throw new RuntimeException(e);
    }
    return new JobApi(client);
  }

  private void onStarted() {
    log.info("onStarted");
    scheduledExecutorService.scheduleAtFixedRate(
        new HeartBeatTask(jobId, stateManager), 0, heartbeatIntervalSeconds, TimeUnit.SECONDS);
    stateManager.updateStartTime(jobId);
    stateManager.updateState(jobId, JobState.RUNNING);
  }

  private void onFinished(boolean success) {
    log.info("onFinished");
    stateManager.updateFinishTime(jobId);
    if (success) {
      stateManager.updateState(jobId, JobState.SUCCEEDED);
      otelEmitter.count(
          METRICS_SCOPE,
          AppConstants.RUN_COUNT,
          1,
          Attributes.of(AttributeKey.stringKey(AppConstants.STATUS), AppConstants.SUCCESS));
    } else {
      stateManager.updateState(jobId, JobState.FAILED);
      otelEmitter.count(
          METRICS_SCOPE,
          AppConstants.RUN_COUNT,
          1,
          Attributes.of(AttributeKey.stringKey(AppConstants.STATUS), AppConstants.FAIL));
    }
    scheduledExecutorService.shutdown();
  }

  @AllArgsConstructor
  static class HeartBeatTask implements Runnable {
    private String jobId;
    private StateManager jobStateManager;

    @Override
    public void run() {
      jobStateManager.sendHeartbeat(jobId);
    }
  }
}
