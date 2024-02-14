package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.jobs.spark.state.StateManager;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;

@Slf4j
public class OpenHouseCatalogSQLTestSparkApp extends BaseSparkApp {
  private static final String DATABASE = "db";
  private static final String TABLE_NAME = "test_sql_app_table";

  protected OpenHouseCatalogSQLTestSparkApp(String jobId, StateManager stateManager) {
    super(jobId, stateManager);
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
    CommandLine cmdLine = createCommandLine(args, Collections.emptyList());
    OpenHouseCatalogSQLTestSparkApp app =
        new OpenHouseCatalogSQLTestSparkApp(getJobId(cmdLine), createStateManager(cmdLine));
    app.run();
  }
}
