package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.jobs.spark.state.StateManager;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

/**
 * Class with main entry point to replication job to trigger airflow run to setup replication for a
 * table with defined replication config
 *
 * <p>Example of invocation: com.linkedin.openhouse.jobs.spark.ReplicationSparkApp --tableName
 * db.testTable
 */
@Slf4j
public class ReplicationSparkApp extends BaseTableSparkApp {
  private final String schedule;
  private final String cluster;
  private final String proxyUser;

  public ReplicationSparkApp(
      String jobId,
      StateManager stateManager,
      String fqtn,
      String schedule,
      String cluster,
      String proxyUser) {
    super(jobId, stateManager, fqtn);
    this.schedule = schedule;
    this.cluster = cluster;
    this.proxyUser = proxyUser;
  }

  @Override
  protected void runInner(Operations ops) {
    log.info(
        "Running ReplicationSparkApp for table {}, with parameters schedule: {}, cluster: {}, proxyUser: {}",
        fqtn,
        schedule,
        cluster,
        proxyUser);
  }

  public static void main(String[] args) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(new Option("s", "schedule", true, "Replication job schedule in cron format"));
    extraOptions.add(
        new Option("p", "proxyUser", true, "Proxy user to run carbon replication job"));
    extraOptions.add(new Option("p", "cluster", true, "Destination cluster for replication"));

    CommandLine cmdLine = createCommandLine(args, extraOptions);
    ReplicationSparkApp app =
        new ReplicationSparkApp(
            getJobId(cmdLine),
            createStateManager(cmdLine),
            cmdLine.getOptionValue("tableName"),
            cmdLine.getOptionValue("schedule"),
            cmdLine.getOptionValue("cluster"),
            cmdLine.getOptionValue("proxyUser"));
    app.run();
  }
}
