package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.client.ssl.HousetablesApiClientFactory;
import com.linkedin.openhouse.common.JobState;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.housetables.client.api.JobApi;
import com.linkedin.openhouse.housetables.client.invoker.ApiClient;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/** Generic abstract class which contains common functionality for any job type */
@Slf4j
@Getter
public abstract class BaseApp {
  protected static final String METRICS_SCOPE = "jobs";
  private final ScheduledExecutorService scheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor();
  protected final String jobId;
  protected final StateManager stateManager;
  protected final OtelEmitter otelEmitter;
  public static final long HEARTBEAT_INTERVAL_SECONDS_DEFAULT = 300;
  private final long heartbeatIntervalSeconds;

  protected BaseApp(
      String jobId,
      StateManager stateManager,
      long heartbeatIntervalSeconds,
      OtelEmitter otelEmitter) {
    this.jobId = jobId;
    this.stateManager = stateManager;
    this.otelEmitter = otelEmitter;
    this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
  }

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

  protected static StateManager createStateManager(
      CommandLine cmdLine, OtelEmitter otelEmitter, String trustStoreLocation) {
    return new StateManager(
        RetryUtil.getJobsStateApiRetryTemplate(),
        createJobApiClient(cmdLine.getOptionValue("storageURL"), otelEmitter, trustStoreLocation));
  }

  protected static StateManager createStateManager(CommandLine cmdLine, OtelEmitter otelEmitter) {
    return new StateManager(
        RetryUtil.getJobsStateApiRetryTemplate(),
        createJobApiClient(cmdLine.getOptionValue("storageURL"), otelEmitter, null));
  }

  protected static String getJobId(CommandLine cmdLine) {
    return cmdLine.getOptionValue("jobId");
  }

  protected static JobApi createJobApiClient(
      String basePath, OtelEmitter otelEmitter, String trustStoreLocation) {
    ApiClient client = null;
    try {
      client =
          HousetablesApiClientFactory.getInstance()
              .createApiClient(basePath, null, trustStoreLocation);
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

  protected void onStarted() {
    log.info("onStarted");
    scheduledExecutorService.scheduleAtFixedRate(
        new HeartBeatTask(jobId, stateManager), 0, heartbeatIntervalSeconds, TimeUnit.SECONDS);
    stateManager.updateStartTime(jobId);
    stateManager.updateState(jobId, JobState.RUNNING);
  }

  protected void onFinished(boolean success) {
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
