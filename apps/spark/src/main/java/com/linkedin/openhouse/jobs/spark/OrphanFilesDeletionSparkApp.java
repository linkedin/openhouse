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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iceberg.actions.DeleteOrphanFiles;

/**
 * Class with main entry point to run as a table orphan files deletion job.
 *
 * <p>Example of invocation: com.linkedin.openhouse.jobs.spark.OrphanFilesDeletionSparkApp
 * --tableName db.testTable
 */
@Slf4j
public class OrphanFilesDeletionSparkApp extends BaseTableSparkApp {
  private final String trashDir;
  private final long ttlSeconds;
  private final boolean skipStaging;

  public OrphanFilesDeletionSparkApp(
      String jobId,
      StateManager stateManager,
      String fqtn,
      String trashDir,
      long ttlSeconds,
      boolean skipStaging,
      OtelEmitter otelEmitter) {
    super(jobId, stateManager, fqtn, otelEmitter);
    this.trashDir = trashDir;
    this.ttlSeconds = ttlSeconds;
    this.skipStaging = skipStaging;
  }

  @Override
  protected void runInner(Operations ops) {
    long olderThanTimestampMillis =
        System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(ttlSeconds);
    log.info(
        "Orphan files deletion app start for table={} with olderThanTimestampMillis={} and skipStaging={}",
        fqtn,
        olderThanTimestampMillis,
        skipStaging);
    DeleteOrphanFiles.Result result =
        ops.deleteOrphanFiles(ops.getTable(fqtn), trashDir, olderThanTimestampMillis, skipStaging);
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

  private static CommandLine prepareCommandLine(String[] args) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(
        new Option("tr", "trashDir", true, "Orphan files staging dir before deletion"));
    extraOptions.add(
        new Option(
            "r",
            "ttl",
            true,
            "How old files should be to be considered orphaned in seconds, minimum 1d is enforced"));
    extraOptions.add(
        new Option(
            "s", "skipStaging", false, "Whether to skip staging orphan files before deletion"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    return cmdLine;
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  public static OrphanFilesDeletionSparkApp createApp(String[] args, OtelEmitter otelEmitter) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(
        new Option("tr", "trashDir", true, "Orphan files staging dir before deletion"));
    extraOptions.add(
        new Option(
            "r",
            "ttl",
            true,
            "How old files should be to be considered orphaned in seconds, minimum 1d is enforced"));
    extraOptions.add(
        new Option(
            "s", "skipStaging", false, "Whether to skip staging orphan files before deletion"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    return new OrphanFilesDeletionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        cmdLine.getOptionValue("tableName"),
        cmdLine.getOptionValue("trashDir"),
        Math.max(
            NumberUtils.toLong(cmdLine.getOptionValue("ttl"), TimeUnit.DAYS.toSeconds(7)),
            TimeUnit.DAYS.toSeconds(1)),
        cmdLine.hasOption("skipStaging"),
        otelEmitter);
  }
}
