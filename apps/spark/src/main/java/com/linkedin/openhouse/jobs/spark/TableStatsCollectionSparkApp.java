package com.linkedin.openhouse.jobs.spark;

import com.google.gson.Gson;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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

    // Initialize event timestamp once for the entire job run to ensure consistency
    long eventTimestampInEpochMs = System.currentTimeMillis();
    log.info("Job event timestamp: {}", eventTimestampInEpochMs);

    IcebergTableStats icebergTableStats = ops.collectTableStats(fqtn);
    publishStats(icebergTableStats);

    // Collect and publish commit events
    try {
      Dataset<Row> commitEventsDF = ops.collectCommitEvents(fqtn, eventTimestampInEpochMs);
      publishCommitEvents(commitEventsDF);
    } catch (Exception e) {
      log.error("Failed to process commit events for table: {}", fqtn, e);
      // Don't fail the entire job if commit events processing fails
    }
  }

  /**
   * Publish table stats.
   *
   * @param icebergTableStats Iceberg table statistics
   */
  protected void publishStats(IcebergTableStats icebergTableStats) {
    log.info("Publishing stats for table: {}", fqtn);
    log.info(new Gson().toJson(icebergTableStats));
  }

  /**
   * Publish commit events. Override this method in li-openhouse to send to Kafka.
   *
   * @param commitEventsDF Dataset containing commit events
   */
  protected void publishCommitEvents(Dataset<Row> commitEventsDF) {
    if (commitEventsDF == null || commitEventsDF.isEmpty()) {
      log.info("No commit events to publish for table: {}", fqtn);
      return;
    }

    log.info("Publishing commit events for table: {}", fqtn);
    String commitEventsJson = commitEventsDF.toJSON().collectAsList().toString();
    log.info("Commit Events: {}", commitEventsJson);
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
