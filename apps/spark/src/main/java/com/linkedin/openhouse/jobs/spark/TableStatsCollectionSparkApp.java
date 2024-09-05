package com.linkedin.openhouse.jobs.spark;

import com.google.gson.Gson;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

/**
 * Class with main entry point to collect Iceberg stats for a table.
 *
 * <p>Example of invocation: com.linkedin.openhouse.jobs.spark.TableStatsCollectionSparkApp
 * --tableName db.testTable --skipStorageStats true
 */
@Slf4j
public class TableStatsCollectionSparkApp extends BaseTableSparkApp {

  private final Boolean skipStorageStatsCollection;

  public TableStatsCollectionSparkApp(
      String jobId, StateManager stateManager, String fqtn, Boolean skipStorageStatsCollection) {
    super(jobId, stateManager, fqtn);
    this.skipStorageStatsCollection = skipStorageStatsCollection;
  }

  @Override
  protected void runInner(Operations ops) {
    log.info("Running TableStatsCollectorApp for table {}", fqtn);

    IcebergTableStats icebergTableStats = ops.collectTableStats(fqtn, skipStorageStatsCollection);
    publishStats(icebergTableStats);
  }

  /**
   * Publish table stats.
   *
   * @param icebergTableStats
   */
  protected void publishStats(IcebergTableStats icebergTableStats) {
    log.info("Publishing stats for table: {}", fqtn);
    log.info(new Gson().toJson(icebergTableStats));
  }

  public static void main(String[] args) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(
        new Option("s", "skipStorageStats", false, "Whether to skip storage stats collection"));

    CommandLine cmdLine = createCommandLine(args, extraOptions);
    TableStatsCollectionSparkApp app =
        new TableStatsCollectionSparkApp(
            getJobId(cmdLine),
            createStateManager(cmdLine),
            cmdLine.getOptionValue("tableName"),
            cmdLine.hasOption("skipStorageStats"));
    app.run();
  }
}
