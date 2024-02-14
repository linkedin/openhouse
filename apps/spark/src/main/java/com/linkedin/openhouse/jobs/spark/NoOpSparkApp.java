package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.jobs.spark.state.StateManager;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;

@Slf4j
public class NoOpSparkApp extends BaseSparkApp {
  public NoOpSparkApp(String jobId, StateManager stateManager) {
    super(jobId, stateManager);
  }

  @Override
  protected void runInner(Operations ops) {
    log.info(String.format("Hello from %s", ops.spark().sparkContext().appName()));
  }

  public static void main(String[] args) {
    CommandLine cmdLine = createCommandLine(args, Collections.emptyList());
    NoOpSparkApp app = new NoOpSparkApp(getJobId(cmdLine), createStateManager(cmdLine));
    app.run();
  }
}
