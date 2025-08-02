package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;

@Slf4j
public class NoOpSparkApp extends BaseSparkApp {
  public NoOpSparkApp(String jobId, StateManager stateManager, OtelEmitter otelEmitter) {
    super(jobId, stateManager, otelEmitter);
  }

  @Override
  protected void runInner(Operations ops) {
    log.info(String.format("Hello from %s", ops.spark().sparkContext().appName()));
  }

  public static void main(String[] args) {
    createApp(args, AppsOtelEmitter.getInstance()).run();
  }

  public static NoOpSparkApp createApp(String[] args, OtelEmitter otelEmitter) {
    CommandLine cmdLine = createCommandLine(args, Collections.emptyList());
    return new NoOpSparkApp(
        getJobId(cmdLine), createStateManager(cmdLine, otelEmitter), otelEmitter);
  }
}
