package com.linkedin.openhouse.jobs.spark;

import com.google.gson.Gson;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitions;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    // Run stats collection, commit events collection, partition events collection, and partition
    // stats collection
    long startTime = System.currentTimeMillis();

    IcebergTableStats icebergStats =
        executeWithTiming(
            "table stats collection",
            () -> ops.collectTableStats(fqtn),
            result -> String.format("%s", fqtn));

    List<CommitEventTable> commitEvents =
        executeWithTiming(
            "commit events collection",
            () -> ops.collectCommitEventTable(fqtn),
            result -> String.format("%s (%d events)", fqtn, result.size()));

    List<CommitEventTablePartitions> partitionEvents =
        executeWithTiming(
            "partition events collection",
            () -> ops.collectCommitEventTablePartitions(fqtn),
            result -> String.format("%s (%d partition events)", fqtn, result.size()));

    List<CommitEventTablePartitionStats> partitionStats =
        executeWithTiming(
            "partition stats collection",
            () -> ops.collectCommitEventTablePartitionStats(fqtn),
            result -> String.format("%s (%d partition stats)", fqtn, result.size()));

    long endTime = System.currentTimeMillis();
    log.info("Total collection time for table: {} in {} ms", fqtn, (endTime - startTime));

    if (icebergStats != null) {
      publishStats(icebergStats);
    } else {
      log.warn("Skipping stats publishing for table: {} due to collection failure", fqtn);
    }

    if (commitEvents != null && !commitEvents.isEmpty()) {
      publishCommitEvents(commitEvents);
    } else {
      log.warn(
          "Skipping commit events publishing for table: {} due to collection failure or no events",
          fqtn);
    }

    if (partitionEvents != null && !partitionEvents.isEmpty()) {
      publishPartitionEvents(partitionEvents);
    } else {
      log.info(
          "Skipping partition events publishing for table: {} "
              + "(unpartitioned table or collection failure or no events)",
          fqtn);
    }

    if (partitionStats != null && !partitionStats.isEmpty()) {
      publishPartitionStats(partitionStats);
    } else {
      log.info(
          "Skipping partition stats publishing for table: {} "
              + "(unpartitioned table or collection failure or no stats)",
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
   * Publish commit events.
   *
   * @param commitEvents List of commit events to publish
   */
  protected void publishCommitEvents(List<CommitEventTable> commitEvents) {
    log.info("Publishing commit events for table: {}", fqtn);
    log.info(new Gson().toJson(commitEvents));
  }

  /**
   * Publish partition-level commit events.
   *
   * @param partitionEvents List of partition events to publish
   */
  protected void publishPartitionEvents(List<CommitEventTablePartitions> partitionEvents) {
    log.info("Publishing partition events for table: {}", fqtn);
    log.info(new Gson().toJson(partitionEvents));
  }

  /**
   * Publish partition-level statistics.
   *
   * <p>This method publishes one stats record per unique partition, where each partition is
   * associated with its latest commit and includes aggregated statistics from data_files.
   *
   * @param partitionStats List of partition statistics to publish
   */
  protected void publishPartitionStats(List<CommitEventTablePartitionStats> partitionStats) {
    log.info("Publishing partition stats for table: {} ({} stats)", fqtn, partitionStats.size());
    log.info(new Gson().toJson(partitionStats));
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  /**
   * Trigger supplier with timing and logging.
   *
   * @param operationName Name of the operation for logging
   * @param supplier The operation to execute
   * @param resultFormatter Function to format the result for logging
   * @param <T> Return type of the operation
   * @return operation result
   */
  private <T> T executeWithTiming(
      String operationName,
      Supplier<T> supplier,
      java.util.function.Function<T, String> resultFormatter) {
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
