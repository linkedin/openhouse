package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;

@Slf4j
public class OpenHouseCatalogSQLTestSparkApp extends BaseSparkApp {
  private static final String DATABASE = "db";
  private static final String TABLE_NAME = "test_sql_app_table";

  protected OpenHouseCatalogSQLTestSparkApp(
      String jobId, StateManager stateManager, OtelEmitter otelEmitter) {
    super(jobId, stateManager, otelEmitter);
  }

  @Override
  protected void runInner(Operations ops) {
    ops.spark().sql(String.format("SHOW TABLES IN %s", DATABASE)).show();
    ops.spark()
        .sql(
            String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (c1 string, c2 string)", DATABASE, TABLE_NAME))
        .show();
    ops.spark().sql(String.format("DESCRIBE TABLE %s.%s", DATABASE, TABLE_NAME)).show();
    ops.spark().sql(String.format("SELECT COUNT(*) FROM %s.%s", DATABASE, TABLE_NAME)).show();
  }

  public static void main(String[] args) {
    createApp(args, AppsOtelEmitter.getInstance()).run();
  }

  public static OpenHouseCatalogSQLTestSparkApp createApp(String[] args, OtelEmitter otelEmitter) {
    CommandLine cmdLine = createCommandLine(args, Collections.emptyList());
    return new OpenHouseCatalogSQLTestSparkApp(
        getJobId(cmdLine), createStateManager(cmdLine, otelEmitter), otelEmitter);
  }
}
