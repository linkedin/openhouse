package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.jobs.spark.state.StateManager;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

/**
 * Class with main entry point to run as a table retention job. The table doesn't have to be time
 * partitioned, but the retention column must be a time column.
 *
 * <p>Example of invocation: com.linkedin.openhouse.jobs.spark.RetentionSparkApp --columnName
 * ingestionTime --tableName openhouse.db.testTable --granularity day --count 14
 *
 * <p>Granularity is one of: minute, hour, day.
 */
@Slf4j
public class RetentionSparkApp extends BaseTableSparkApp {
  private final String columnName;
  private final String columnPattern;
  private final String granularity;
  private final int count;

  public RetentionSparkApp(
      String jobId,
      StateManager stateManager,
      String fqtn,
      String columnName,
      String columnPattern,
      String granularity,
      int count) {
    super(jobId, stateManager, fqtn);
    this.columnName = columnName;
    this.columnPattern = columnPattern;
    this.granularity = granularity;
    this.count = count;
  }

  @Override
  protected void runInner(Operations ops) {
    log.info(
        "Retention app start for table {}, column {}, {}, ttl={} {}s",
        fqtn,
        columnName,
        columnPattern,
        count,
        granularity);
    ops.runRetention(fqtn, columnName, columnPattern, granularity, count);
  }

  public static void main(String[] args) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(new Option("cn", "columnName", true, "Retention column name"));
    extraOptions.add(new Option("cp", "columnPattern", true, "Retention column pattern"));
    extraOptions.add(new Option("g", "granularity", true, "Granularity: day, week"));
    extraOptions.add(new Option("c", "count", true, "Retain last <count> <granularity>s"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    RetentionSparkApp app =
        new RetentionSparkApp(
            getJobId(cmdLine),
            createStateManager(cmdLine),
            cmdLine.getOptionValue("tableName"),
            cmdLine.getOptionValue("columnName"),
            cmdLine.getOptionValue("columnPattern", ""),
            cmdLine.getOptionValue("granularity"),
            Integer.parseInt(cmdLine.getOptionValue("count")));
    app.run();
  }
}
