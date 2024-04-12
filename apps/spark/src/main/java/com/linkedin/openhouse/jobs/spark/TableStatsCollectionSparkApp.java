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
 * --tableName db.testTable
 */
@Slf4j
public class TableStatsCollectionSparkApp extends BaseTableSparkApp {

  public TableStatsCollectionSparkApp(String jobId, StateManager stateManager, String fqtn) {
    super(jobId, stateManager, fqtn);
  }

  @Override
  protected void runInner(Operations ops) {
    log.info("Running TableStatsCollectorApp for table {}", fqtn);

    IcebergTableStats icebergTableStats = ops.collectTableStats(fqtn);
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
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    TableStatsCollectionSparkApp app =
        new TableStatsCollectionSparkApp(
            getJobId(cmdLine), createStateManager(cmdLine), cmdLine.getOptionValue("tableName"));
    app.run();
  }
}
