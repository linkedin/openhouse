package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.jobs.spark.state.StateManager;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

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

  public static class DEFAULT_CONFIGURATION {
    public static final int MAX_AGE = 3;
    public static final String GRANULARITY = "DAYS";
    public static final int VERSIONS = 0;
  }

  public SnapshotsExpirationSparkApp(
      String jobId,
      StateManager stateManager,
      String fqtn,
      int maxAge,
      String granularity,
      int versions) {
    super(jobId, stateManager, fqtn);
    // By default, always enforce a time to live for snapshots even if unconfigured
    if (maxAge == 0) {
      this.maxAge = DEFAULT_CONFIGURATION.MAX_AGE;
      this.granularity = DEFAULT_CONFIGURATION.GRANULARITY;
    } else {
      this.maxAge = maxAge;
      this.granularity = granularity;
    }
    this.versions = versions;
  }

  @Override
  protected void runInner(Operations ops) {
    log.info(
        "Snapshot expiration app start for table {}, expiring older than {} {}s or with more than {} versions",
        fqtn,
        maxAge,
        granularity,
        versions);
    ops.expireSnapshots(fqtn, maxAge, granularity, versions);
  }

  public static void main(String[] args) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(
        new Option("a", "maxAge", true, "Delete snapshots older than <maxAge> <granularity>s"));
    extraOptions.add(new Option("g", "granularity", true, "Granularity: day"));
    extraOptions.add(
        new Option("v", "versions", true, "Number of versions to keep after snapshot expiration"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);

    SnapshotsExpirationSparkApp app =
        new SnapshotsExpirationSparkApp(
            getJobId(cmdLine),
            createStateManager(cmdLine),
            cmdLine.getOptionValue("tableName"),
            Integer.parseInt(cmdLine.getOptionValue("maxAge", "0")),
            cmdLine.getOptionValue("granularity", ""),
            Integer.parseInt(cmdLine.getOptionValue("versions", "0")));
    app.run();
  }
}
