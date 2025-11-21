package com.linkedin.openhouse.jobs.spark;

import com.google.gson.Gson;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

/**
 * Class with main entry point to collect Iceberg stats for a table.
 *
 * <p>Example of invocation: com.linkedin.openhouse.jobs.spark.TableStatsCollectionSparkApp
 * --tableName db.testTable
 */
@Slf4j
public class TableStatsCollectionSparkApp extends BaseTableSparkApp {

  public TableStatsCollectionSparkApp(
      String jobId, StateManager stateManager, String fqtn, OtelEmitter otelEmitter) {
    super(jobId, stateManager, fqtn, otelEmitter);
  }

  @Override
  protected void runInner(Operations ops) {
    log.info("Running TableStatsCollectorApp for table {}", fqtn);

    // Run stats collection and commit events collection in parallel
    long startTime = System.currentTimeMillis();

    CompletableFuture<IcebergTableStats> statsFuture =
        CompletableFuture.supplyAsync(
            () -> {
              long statsStartTime = System.currentTimeMillis();
              log.info("Starting table stats collection for table: {}", fqtn);
              IcebergTableStats stats = ops.collectTableStats(fqtn);
              long statsEndTime = System.currentTimeMillis();
              log.info(
                  "Completed table stats collection for table: {} in {} ms",
                  fqtn,
                  (statsEndTime - statsStartTime));
              return stats;
            });

    CompletableFuture<List<CommitEventTable>> commitEventsFuture =
        CompletableFuture.supplyAsync(
            () -> {
              long commitStartTime = System.currentTimeMillis();
              log.info("Starting commit events collection for table: {}", fqtn);
              List<CommitEventTable> events = ops.collectCommitEventTable(fqtn);
              long commitEndTime = System.currentTimeMillis();
              log.info(
                  "Completed commit events collection for table: {} in {} ms ({} events)",
                  fqtn,
                  (commitEndTime - commitStartTime),
                  events.size());
              return events;
            });

    // Wait for both to complete
    CompletableFuture.allOf(statsFuture, commitEventsFuture).join();

    long endTime = System.currentTimeMillis();
    log.info(
        "Total collection time for table: {} in {} ms (parallel execution)",
        fqtn,
        (endTime - startTime));

    // Publish results
    IcebergTableStats icebergTableStats = statsFuture.join();
    publishStats(icebergTableStats);

    List<CommitEventTable> commitEvents = commitEventsFuture.join();
    publishCommitEvents(commitEvents);
  }

  /**
   * Publish table stats.
   *
   * @param icebergTableStats
   */
  protected void publishStats(IcebergTableStats icebergTableStats) {
    log.info("Publishing stats for table: {}", fqtn);
    log.info(new Gson().toJson(icebergTableStats));
  }

  /**
   * Publish commit events. Override this method in li-openhouse to send to Kafka.
   *
   * @param commitEvents List of commit events to publish
   */
  protected void publishCommitEvents(List<CommitEventTable> commitEvents) {
    // Set event timestamp at publish time
    long eventTimestampInEpochMs = System.currentTimeMillis();
    commitEvents.forEach(event -> event.setEventTimestampMs(eventTimestampInEpochMs));

    log.info("Publishing commit events for table: {}", fqtn);
    log.info(new Gson().toJson(commitEvents));
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  public static TableStatsCollectionSparkApp createApp(String[] args, OtelEmitter otelEmitter) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(new Option("t", "tableName", true, "Fully-qualified table name"));
    CommandLine cmdLine = createCommandLine(args, extraOptions);
    return new TableStatsCollectionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        cmdLine.getOptionValue("tableName"),
        otelEmitter);
  }
}
