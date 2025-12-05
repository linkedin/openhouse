package com.linkedin.openhouse.jobs.spark;

import com.google.common.collect.Lists;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;

/**
 * Class with main entry point to run as a table orphan files deletion job.
 *
 * <p>Example of invocation: com.linkedin.openhouse.jobs.spark.OrphanFilesDeletionSparkApp
 * --tableName db.testTable
 */
@Slf4j
public class OrphanFilesDeletionSparkApp extends BaseTableSparkApp {
  private final long ttlSeconds;
  private final Operations.BackupManager backupManager;

  public OrphanFilesDeletionSparkApp(
      String jobId,
      StateManager stateManager,
      String fqtn,
      long ttlSeconds,
      OtelEmitter otelEmitter,
      Operations.BackupManager backupManager) {
    super(jobId, stateManager, fqtn, otelEmitter);
    this.ttlSeconds = ttlSeconds;
    this.backupManager = backupManager;
  }

  @Override
  protected void runInner(Operations ops) {
    long olderThanTimestampMillis =
        System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(ttlSeconds);
    Table table = ops.getTable(fqtn);
    boolean backupEnabled =
        Boolean.parseBoolean(
            table.properties().getOrDefault(AppConstants.BACKUP_ENABLED_KEY, "false"));
    log.info(
        "Orphan files deletion app start for table={} with olderThanTimestampMillis={} backupEnabled={} and backupDir={}",
        fqtn,
        olderThanTimestampMillis,
        backupEnabled,
        backupManager.backupDir);
    DeleteOrphanFiles.Result result =
        ops.deleteOrphanFiles(table, olderThanTimestampMillis, backupManager, backupEnabled);
    List<String> orphanFileLocations = Lists.newArrayList(result.orphanFileLocations().iterator());
    log.info(
        "Detected {} orphan files older than {}ms",
        orphanFileLocations.size(),
        olderThanTimestampMillis);
    otelEmitter.count(
        METRICS_SCOPE,
        AppConstants.ORPHAN_FILE_COUNT,
        orphanFileLocations.size(),
        Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), fqtn));
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Collections.singletonList(DefaultOtelConfig.getOpenTelemetry()));
    CommandLine cmdLine = createCommandLine(args);
    Operations.BackupManager backupManager =
        Operations.BackupManager.builder()
            .backupDir(cmdLine.getOptionValue("backupDir", ".backup"))
            .build();
    OrphanFilesDeletionSparkApp app =
        new OrphanFilesDeletionSparkApp(
            getJobId(cmdLine),
            createStateManager(cmdLine, otelEmitter),
            cmdLine.getOptionValue("tableName"),
            Math.max(
                NumberUtils.toLong(cmdLine.getOptionValue("ttl"), TimeUnit.DAYS.toSeconds(7)),
                TimeUnit.DAYS.toSeconds(1)),
            otelEmitter,
            backupManager);
    app.run();
  }

  protected static CommandLine createCommandLine(String[] args) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(
        new Option(
            "r",
            "ttl",
            true,
            "How old files should be to be considered orphaned in seconds, minimum 1d is enforced"));
    extraOptions.add(new Option("b", "backupDir", true, "Backup directory"));
    return createCommandLine(args, extraOptions);
  }
}
