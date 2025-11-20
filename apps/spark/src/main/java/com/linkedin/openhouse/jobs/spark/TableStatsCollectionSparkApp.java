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

    IcebergTableStats icebergTableStats = ops.collectTableStats(fqtn);
    publishStats(icebergTableStats);

    // Collect and publish commit events
    List<CommitEventTable> commitEvents = ops.collectCommitEventTable(fqtn);

    // Set event timestamp right before publishing (represents when event is published)
    long eventTimestampInEpochMs = System.currentTimeMillis();
    commitEvents.forEach(event -> event.setEventTimestampMs(eventTimestampInEpochMs));

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
