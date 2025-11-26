package com.linkedin.openhouse.jobs.spark;

import com.google.gson.Gson;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitions;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
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

    // Run stats collection, commit events collection, and partition events collection in parallel
    long startTime = System.currentTimeMillis();

    CompletableFuture<IcebergTableStats> statsFuture =
        executeWithTimingAsync(
            "table stats collection",
            () -> ops.collectTableStats(fqtn),
            result -> String.format("%s", fqtn));

    CompletableFuture<List<CommitEventTable>> commitEventsFuture =
        executeWithTimingAsync(
            "commit events collection",
            () -> ops.collectCommitEventTable(fqtn),
            result -> String.format("%s (%d events)", fqtn, result.size()));

    CompletableFuture<List<CommitEventTablePartitions>> partitionEventsFuture =
        executeWithTimingAsync(
            "partition events collection",
            () -> ops.collectCommitEventTablePartitions(fqtn),
            result -> String.format("%s (%d partition events)", fqtn, result.size()));

    // Wait for all three to complete
    CompletableFuture.allOf(statsFuture, commitEventsFuture, partitionEventsFuture).join();

    long endTime = System.currentTimeMillis();
    log.info(
        "Total collection time for table: {} in {} ms (parallel execution)",
        fqtn,
        (endTime - startTime));

    // Publish results
    IcebergTableStats icebergTableStats = statsFuture.join();
    if (icebergTableStats != null) {
      publishStats(icebergTableStats);
    } else {
      log.warn("Skipping stats publishing for table: {} due to collection failure", fqtn);
    }

    List<CommitEventTable> commitEvents = commitEventsFuture.join();
    if (commitEvents != null && !commitEvents.isEmpty()) {
      publishCommitEvents(commitEvents);
    } else {
      log.warn(
          "Skipping commit events publishing for table: {} due to collection failure or no events",
          fqtn);
    }

    List<CommitEventTablePartitions> partitionEvents = partitionEventsFuture.join();
    if (partitionEvents != null && !partitionEvents.isEmpty()) {
      publishPartitionEvents(partitionEvents);
    } else {
      log.info(
          "Skipping partition events publishing for table: {} "
              + "(unpartitioned table or collection failure or no events)",
          fqtn);
    }
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

  /**
   * Publish partition-level commit events. Override this method in li-openhouse to send to Kafka.
   *
   * @param partitionEvents List of partition events to publish
   */
  protected void publishPartitionEvents(List<CommitEventTablePartitions> partitionEvents) {
    // Set event timestamp at publish time
    long eventTimestampInEpochMs = System.currentTimeMillis();
    partitionEvents.forEach(event -> event.setEventTimestampMs(eventTimestampInEpochMs));

    log.info("Publishing partition events for table: {}", fqtn);
    log.info(new Gson().toJson(partitionEvents));
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  /**
   * Execute a supplier asynchronously with timing and logging.
   *
   * @param operationName Name of the operation for logging
   * @param supplier The operation to execute
   * @param resultFormatter Function to format the result for logging
   * @param <T> Return type of the operation
   * @return CompletableFuture wrapping the operation result
   */
  private <T> CompletableFuture<T> executeWithTimingAsync(
      String operationName,
      Supplier<T> supplier,
      java.util.function.Function<T, String> resultFormatter) {
    return CompletableFuture.supplyAsync(
        () -> {
          long startTime = System.currentTimeMillis();
          log.info("Starting {} for table: {}", operationName, fqtn);
          T result = supplier.get();
          long endTime = System.currentTimeMillis();

          String resultDescription =
              (result != null) ? resultFormatter.apply(result) : "null (collection failed)";
          log.info(
              "Completed {} for table: {} in {} ms",
              operationName,
              resultDescription,
              (endTime - startTime));
          return result;
        });
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
