package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.iceberg.actions.ExpireSnapshots;

/**
 * Class with main entry point to run as a table snapshot expiration job. Snapshots for table which
 * are older than provided count of granularities are deleted. Current snapshot is always preserved.
 *
 * <p>Example of invocation: com.linkedin.openhouse.jobs.spark.SnapshotsExpirationSparkApp
 * --tableName db.testTable --maxAge 3 --granularity day --versions 10
 */
@Slf4j
public class SnapshotsExpirationSparkApp extends BaseTableSparkApp {
  private final String granularity;
  private final int maxAge;
  private final int versions;
  private final boolean deleteFiles;

  public static class DEFAULT_CONFIGURATION {
    public static final int MAX_AGE = 3;
    public static final String GRANULARITY = ChronoUnit.DAYS.toString();
    public static final int VERSIONS = 0;
    public static final boolean DELETE_FILES = false;
  }

  public SnapshotsExpirationSparkApp(
      String jobId,
      StateManager stateManager,
      String fqtn,
      int maxAge,
      String granularity,
      int versions,
      boolean deleteFiles,
      OtelEmitter otelEmitter) {
    super(jobId, stateManager, fqtn, otelEmitter);
    // By default, always enforce a time to live for snapshots even if unconfigured
    if (maxAge == 0) {
      this.maxAge = DEFAULT_CONFIGURATION.MAX_AGE;
      this.granularity = DEFAULT_CONFIGURATION.GRANULARITY;
    } else {
      this.maxAge = maxAge;
      this.granularity = granularity;
    }
    this.versions = versions;
    this.deleteFiles = deleteFiles;
  }

  @Override
  protected void runInner(Operations ops) {
    log.info(
        "Snapshot expiration app start for table {}, expiring older than {} {}s or with more than {} versions, deleteFiles={}",
        fqtn,
        maxAge,
        granularity,
        versions,
        deleteFiles);

    long startTime = System.currentTimeMillis();
    ExpireSnapshots.Result result =
        ops.expireSnapshots(fqtn, maxAge, granularity, versions, deleteFiles);
    long duration = System.currentTimeMillis() - startTime;

    // Log results
    log.info(
        "Snapshot expiration completed for table {}. Deleted {} data files, {} equality delete files, {} position delete files, {} manifests, {} manifest lists",
        fqtn,
        result.deletedDataFilesCount(),
        result.deletedEqualityDeleteFilesCount(),
        result.deletedPositionDeleteFilesCount(),
        result.deletedManifestsCount(),
        result.deletedManifestListsCount());

    // Emit metrics
    recordMetrics(duration);
  }

  private void recordMetrics(long duration) {
    io.opentelemetry.api.common.Attributes attributes =
        io.opentelemetry.api.common.Attributes.of(
            io.opentelemetry.api.common.AttributeKey.stringKey(AppConstants.TABLE_NAME),
            fqtn,
            io.opentelemetry.api.common.AttributeKey.booleanKey(AppConstants.DELETE_FILES_ENABLED),
            deleteFiles);
    otelEmitter.time(
        SnapshotsExpirationSparkApp.class.getName(),
        AppConstants.SNAPSHOTS_EXPIRATION_DURATION,
        duration,
        attributes);
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  public static SnapshotsExpirationSparkApp createApp(String[] args, OtelEmitter otelEmitter) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(
        new Option("a", "maxAge", true, "Delete snapshots older than <maxAge> <granularity>s"));
    extraOptions.add(new Option("g", "granularity", true, "Granularity: day"));
    extraOptions.add(
        new Option("v", "versions", true, "Number of versions to keep after snapshot expiration"));
    extraOptions.add(
        new Option(
            "d",
            "deleteFiles",
            false,
            "Delete expired snapshot files (data, manifests, manifest lists)"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    return new SnapshotsExpirationSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        cmdLine.getOptionValue("tableName"),
        Integer.parseInt(cmdLine.getOptionValue("maxAge", "0")),
        cmdLine.getOptionValue("granularity", ""),
        Integer.parseInt(cmdLine.getOptionValue("versions", "0")),
        cmdLine.hasOption("deleteFiles"),
        otelEmitter);
  }
}
