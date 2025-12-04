package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.iceberg.Table;

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
  private final String backupDir;

  public RetentionSparkApp(
      String jobId,
      StateManager stateManager,
      String fqtn,
      String columnName,
      String columnPattern,
      String granularity,
      int count,
      OtelEmitter otelEmitter,
      String backupDir) {
    super(jobId, stateManager, fqtn, otelEmitter);
    this.columnName = columnName;
    this.columnPattern = columnPattern;
    this.granularity = granularity;
    this.count = count;
    this.backupDir = backupDir;
  }

  @Override
  protected void runInner(Operations ops) {
    Table table = ops.getTable(fqtn);
    boolean backupEnabled =
        Boolean.parseBoolean(
            table.properties().getOrDefault(AppConstants.BACKUP_ENABLED_KEY, "false"));
    ZonedDateTime now = ZonedDateTime.now();
    log.info(
        "Retention app start for table {}, column {}, {}, ttl={} {}s, backupEnabled={}, backupDir={}, ts={}",
        fqtn,
        columnName,
        columnPattern,
        count,
        granularity,
        backupEnabled,
        backupDir,
        now);
    ops.runRetention(
        fqtn, columnName, columnPattern, granularity, count, backupEnabled, backupDir, now);
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  public static RetentionSparkApp createApp(String[] args, OtelEmitter otelEmitter) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(new Option("cn", "columnName", true, "Retention column name"));
    extraOptions.add(new Option("cp", "columnPattern", true, "Retention column pattern"));
    extraOptions.add(new Option("g", "granularity", true, "Granularity: day, week"));
    extraOptions.add(new Option("c", "count", true, "Retain last <count> <granularity>s"));
    extraOptions.add(new Option("b", "backupDir", true, "Backup directory for deleted data"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    return new RetentionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        cmdLine.getOptionValue("tableName"),
        cmdLine.getOptionValue("columnName"),
        cmdLine.getOptionValue("columnPattern", ""),
        cmdLine.getOptionValue("granularity"),
        Integer.parseInt(cmdLine.getOptionValue("count")),
        otelEmitter,
        cmdLine.getOptionValue("backupDir", ".backup"));
  }
}
