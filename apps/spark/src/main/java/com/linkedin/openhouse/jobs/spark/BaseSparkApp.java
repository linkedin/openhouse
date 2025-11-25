package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

/**
 * Base app class implemented common operations, e.g. heartbeats, state update, Spark session
 * creation, args parsing.
 */
@Slf4j
public abstract class BaseSparkApp extends BaseJob {
  protected static final String METRICS_SCOPE = BaseSparkApp.class.getName();
  private final ScheduledExecutorService scheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor();

  protected BaseSparkApp(String jobId, StateManager stateManager, OtelEmitter otelEmitter) {
    super(jobId, stateManager, HEARTBEAT_INTERVAL_SECONDS_DEFAULT, otelEmitter);
  }

  protected BaseSparkApp(
      String jobId,
      StateManager stateManager,
      long heartbeatIntervalSeconds,
      OtelEmitter otelEmitter) {
    super(jobId, stateManager, heartbeatIntervalSeconds, otelEmitter);
  }

  public void run() {
    log.info("Running");
    String className = this.getClass().getSimpleName();
    boolean isSuccess = true;
    try (Operations ops =
        Operations.withCatalog(
            SparkSession.builder().appName(className).getOrCreate(), otelEmitter)) {
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
}
