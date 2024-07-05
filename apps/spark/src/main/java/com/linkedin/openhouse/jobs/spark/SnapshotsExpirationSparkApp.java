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
  private final int count;

  public SnapshotsExpirationSparkApp(
      String jobId, StateManager stateManager, String fqtn, String granularity, int count) {
    super(jobId, stateManager, fqtn);
    this.granularity = granularity;
    this.count = count;
  }

  @Override
  protected void runInner(Operations ops) {
    log.info(
        "Snapshot expiration app start for table {}, expiring older than {} {}s",
        fqtn,
        count,
        granularity);
    long expireBeforeTimestampMs = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(count);
    log.info("Expire snapshots before timestamp ms {}", expireBeforeTimestampMs);
    ops.expireSnapshots(fqtn, expireBeforeTimestampMs);
  }

  public static void main(String[] args) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    extraOptions.add(new Option("g", "granularity", true, "Granularity: day"));
    extraOptions.add(
        new Option("c", "count", true, "Delete snapshots older than <count> <granularity>s"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    SnapshotsExpirationSparkApp app =
        new SnapshotsExpirationSparkApp(
            getJobId(cmdLine),
            createStateManager(cmdLine),
            cmdLine.getOptionValue("tableName"),
            cmdLine.getOptionValue("granularity"),
            Integer.parseInt(cmdLine.getOptionValue("count")));
    app.run();
  }
}
