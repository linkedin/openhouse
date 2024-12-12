package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.jobs.spark.state.StateManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

/**
 * Class with main entry point to run as a table snapshot expiration job. Snapshots for table which
 * are older than provided count of granularities are deleted. Current snapshot is always preserved.
 *
 * <p>Example of invocation: com.linkedin.openhouse.jobs.spark.SnapshotsExpirationSparkApp
 * --tableName db.testTable --count 7 --granularity day
 */
@Slf4j
public class SnapshotsExpirationSparkApp extends BaseTableSparkApp {
  private final String granularity;
  private final int maxAge;
  private final int minVersions;

  private static final String DEFAULT_MAX_AGE = "3";

  private static final String DEFAULT_GRANULARITY = "days";

  private static final String DEFAULT_MIN_VERSIONS = "100";

  public SnapshotsExpirationSparkApp(
      String jobId,
      StateManager stateManager,
      String fqtn,
      int maxAge,
      String granularity,
      int minVersions) {
    super(jobId, stateManager, fqtn);
    this.granularity = granularity;
    this.maxAge = maxAge;
    this.minVersions = minVersions;
  }

  @Override
  protected void runInner(Operations ops) {
    log.info(
        "Snapshot expiration app start for table {}, expiring older than {} {}s or with more than {} versions",
        fqtn,
        maxAge,
        granularity,
        minVersions);
    long expireBeforeTimestampMs = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(maxAge);
    log.info("Expire snapshots before timestamp ms {}", expireBeforeTimestampMs);
    ops.expireSnapshots(fqtn, expireBeforeTimestampMs, minVersions);
  }

  public static void main(String[] args) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(
        new Option("a", "maxAge", true, "Delete snapshots older than <maxAge> <granularity>s"));
    extraOptions.add(new Option("g", "granularity", true, "Granularity: day"));
    extraOptions.add(new Option("v", "minVersions", true, "Minimum number of versions to keep"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    SnapshotsExpirationSparkApp app =
        new SnapshotsExpirationSparkApp(
            getJobId(cmdLine),
            createStateManager(cmdLine),
            cmdLine.getOptionValue("tableName"),
            Integer.parseInt(cmdLine.getOptionValue("maxAge", DEFAULT_MAX_AGE)),
            cmdLine.getOptionValue("granularity", DEFAULT_GRANULARITY),
            Integer.parseInt(cmdLine.getOptionValue("minVersions", DEFAULT_MIN_VERSIONS)));
    app.run();
  }
}
