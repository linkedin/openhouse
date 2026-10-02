package com.linkedin.openhouse.jobs.spark;

import com.google.gson.Gson;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.common.stats.model.CommitEventTable;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitionStats;
import com.linkedin.openhouse.common.stats.model.CommitEventTablePartitions;
import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.jobs.spark.optimizer.OptimizerServiceClient;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.optimizer.client.model.UpdateOperationRequest;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.lang3.StringUtils;

/**
 * Batched table-stats-collection Spark app. One Spark job processes a list of {@code (table,
 * operationId)} pairs that the optimizer scheduler bin-packed (by file count) into a single batch.
 * Each table is handled by a worker thread; per-table failures are caught and reported back
 * independently — the job continues for the remaining tables and exits 0 if at least one table
 * succeeds.
 *
 * <p>This is the multi-table counterpart of {@link TableStatsCollectionSparkApp}. The single-table
 * app remains the deployment unit when bin size is 1, and stays the canonical reference for the
 * actual collection/publish logic. Unlike {@link BatchedOrphanFilesDeletionSparkApp}, stats
 * collection does not mutate the table, so there is no post-job table-state validation.
 *
 * <p>Example invocation:
 *
 * <pre>{@code
 * com.linkedin.openhouse.jobs.spark.BatchedTableStatsCollectionSparkApp \
 *   --tableNames db.t1,db.t2,db.t3 \
 *   --operationIds op-uuid-1,op-uuid-2,op-uuid-3 \
 *   --tableUuids tab-uuid-1,tab-uuid-2,tab-uuid-3 \
 *   --resultsEndpoint http://optimizer.svc:8080 \
 *   --driverParallelism 4
 * }</pre>
 */
@Slf4j
public class BatchedTableStatsCollectionSparkApp extends BaseSparkApp {

  private final List<BatchEntry> entries;
  private final String resultsEndpoint;
  private final int driverParallelism;

  public BatchedTableStatsCollectionSparkApp(
      String jobId,
      StateManager stateManager,
      OtelEmitter otelEmitter,
      List<BatchEntry> entries,
      String resultsEndpoint,
      int driverParallelism) {
    super(jobId, stateManager, otelEmitter);
    this.entries = entries;
    this.resultsEndpoint = resultsEndpoint;
    this.driverParallelism = Math.max(1, driverParallelism);
  }

  @Override
  protected void runInner(Operations ops) {
    log.info(
        "Batched stats collection start: entries={} driverParallelism={} resultsEndpoint={}",
        entries.size(),
        driverParallelism,
        resultsEndpoint);

    if (entries.isEmpty()) {
      log.warn("Batched stats collection invoked with no entries; nothing to do");
      return;
    }

    Optional<OptimizerServiceClient> client = newOptimizerClient();
    int successCount = runBatch(ops, client);

    int failureCount = entries.size() - successCount;
    log.info(
        "Batched stats collection finished: total={} success={} failed={}",
        entries.size(),
        successCount,
        failureCount);

    if (successCount == 0) {
      throw new RuntimeException(
          String.format("All %d operations in batch failed", entries.size()));
    }
  }

  private int runBatch(Operations ops, Optional<OptimizerServiceClient> client) {
    ExecutorService pool = Executors.newFixedThreadPool(driverParallelism);
    try {
      // Two-phase pipeline: submit every worker first (so they run concurrently), then await each.
      // Pairing each Future with its BatchEntry via AbstractMap.SimpleImmutableEntry.
      List<Map.Entry<BatchEntry, Future<Boolean>>> submissions =
          entries.stream()
              .map(
                  entry ->
                      new AbstractMap.SimpleImmutableEntry<>(
                          entry, pool.submit(new TableWorker(ops, entry, client))))
              .collect(Collectors.toList());
      return submissions.stream()
          .mapToInt(submission -> awaitOne(submission.getKey(), submission.getValue(), client))
          .sum();
    } finally {
      shutdownPool(pool);
    }
  }

  private int awaitOne(
      BatchEntry entry, Future<Boolean> future, Optional<OptimizerServiceClient> client) {
    try {
      return Boolean.TRUE.equals(future.get()) ? 1 : 0;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Worker interrupted: fqtn={}", entry.getFqtn(), e);
      otelEmitter.count(
          METRICS_SCOPE,
          "optimizer_batch_interrupted",
          1,
          Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), entry.getFqtn()));
      return 0;
    } catch (ExecutionException e) {
      // The worker catches Throwable internally and always reports its own result, so reaching
      // here means the worker itself leaked an exception. Be defensive: post FAILED so the
      // operation row doesn't sit SCHEDULED until the stale-timeout.
      log.error(
          "Worker threw outside its own catch for fqtn={} — reporting FAILED",
          entry.getFqtn(),
          e.getCause());
      reportResult(entry, UpdateOperationRequest.StatusEnum.FAILED, client);
      return 0;
    }
  }

  private void shutdownPool(ExecutorService pool) {
    pool.shutdown();
    try {
      if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
        pool.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      pool.shutdownNow();
    }
  }

  /**
   * Returns a client bound to {@link #resultsEndpoint}, or empty when the endpoint was not
   * configured — in that case the legacy {@link
   * com.linkedin.openhouse.jobs.scheduler.JobsScheduler} is the caller and reports lifecycle via
   * HTS; the per-operation optimizer callback is skipped.
   */
  protected Optional<OptimizerServiceClient> newOptimizerClient() {
    return Optional.ofNullable(resultsEndpoint).map(OptimizerServiceClient::new);
  }

  /**
   * POST the per-operation outcome to the Optimizer Service via the generated client. No-op when
   * {@code client} is empty (the legacy scheduler-driven path; lifecycle is already tracked via
   * HTS). When the call exhausts retries we log + count and leave the operation row at SCHEDULED so
   * the Analyzer's stale-timeout can re-queue it.
   */
  private void reportResult(
      BatchEntry entry,
      UpdateOperationRequest.StatusEnum status,
      Optional<OptimizerServiceClient> client) {
    if (!client.isPresent()) {
      return;
    }
    UpdateOperationRequest body =
        new UpdateOperationRequest()
            .operationId(entry.getOperationId().orElse(null))
            .status(status)
            .tableUuid(entry.getTableUuid().orElse(null))
            .databaseName(entry.getDatabaseName())
            .tableName(entry.getTableName())
            .operationType(UpdateOperationRequest.OperationTypeEnum.TABLE_STATS_COLLECTION);
    if (!client.get().updateOperation(entry.getOperationId().orElse(null), body).isPresent()) {
      log.error(
          "Failed to report operation result after retries; row will stay SCHEDULED until stale-timeout: operationId={} fqtn={}",
          entry.getOperationId().orElse(null),
          entry.getFqtn());
      otelEmitter.count(
          METRICS_SCOPE,
          "optimizer_update_failed",
          1,
          Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), entry.getFqtn()));
    }
  }

  // --- Publish hooks (mirror the single-table app; log via Gson). Overridable for tests. ---

  protected void publishStats(String fqtn, IcebergTableStats icebergTableStats) {
    log.info("Publishing stats for table: {}", fqtn);
    log.info(new Gson().toJson(icebergTableStats));
  }

  protected void publishCommitEvents(String fqtn, List<CommitEventTable> commitEvents) {
    log.info("Publishing commit events for table: {}", fqtn);
    log.info(new Gson().toJson(commitEvents));
  }

  protected void publishPartitionEvents(
      String fqtn, List<CommitEventTablePartitions> partitionEvents) {
    log.info("Publishing partition events for table: {}", fqtn);
    log.info(new Gson().toJson(partitionEvents));
  }

  protected void publishPartitionStats(
      String fqtn, List<CommitEventTablePartitionStats> partitionStats) {
    log.info("Publishing partition stats for table: {} ({} stats)", fqtn, partitionStats.size());
    log.info(new Gson().toJson(partitionStats));
  }

  /** One unit of work in a batched stats-collection job. */
  private final class TableWorker implements Callable<Boolean> {
    private final Operations ops;
    private final BatchEntry entry;
    private final Optional<OptimizerServiceClient> client;

    TableWorker(Operations ops, BatchEntry entry, Optional<OptimizerServiceClient> client) {
      this.ops = ops;
      this.entry = entry;
      this.client = client;
    }

    @Override
    public Boolean call() {
      String fqtn = entry.getFqtn();
      UpdateOperationRequest.StatusEnum status = UpdateOperationRequest.StatusEnum.FAILED;
      try {
        log.info(
            "Stats collection start: fqtn={} operationId={}",
            fqtn,
            entry.getOperationId().orElse(""));
        collectAndPublish(fqtn);
        status = UpdateOperationRequest.StatusEnum.SUCCESS;
        log.info("Stats collection success: fqtn={}", fqtn);
      } catch (Throwable t) {
        log.error(
            "Stats collection failed: fqtn={} operationId={}",
            fqtn,
            entry.getOperationId().orElse(""),
            t);
      } finally {
        // Defensive: reportResult must not throw out of the finally block, since that would mask
        // the original failure and propagate up to awaitOne, which would then report FAILED again.
        try {
          reportResult(entry, status, client);
        } catch (Throwable t) {
          log.error(
              "reportResult itself threw; operation row will stay SCHEDULED until stale-timeout: fqtn={}",
              fqtn,
              t);
        }
      }
      return status == UpdateOperationRequest.StatusEnum.SUCCESS;
    }

    /**
     * Collect and publish stats for a single table, mirroring {@link TableStatsCollectionSparkApp}.
     * Core table stats are required — a null result is treated as a failed operation (so the
     * Analyzer's failure cadence retries it). Commit-/partition-level artifacts are best-effort:
     * empty results are skipped, but a thrown exception fails the operation for this table.
     */
    private void collectAndPublish(String fqtn) {
      IcebergTableStats icebergStats = ops.collectTableStats(fqtn);
      if (icebergStats == null) {
        throw new IllegalStateException("Table stats collection returned null for " + fqtn);
      }
      publishStats(fqtn, icebergStats);

      List<CommitEventTable> commitEvents = ops.collectCommitEventTable(fqtn);
      if (commitEvents != null && !commitEvents.isEmpty()) {
        publishCommitEvents(fqtn, commitEvents);
      } else {
        log.info("No commit events to publish for table: {}", fqtn);
      }

      List<CommitEventTablePartitions> partitionEvents =
          ops.collectCommitEventTablePartitions(fqtn);
      if (partitionEvents != null && !partitionEvents.isEmpty()) {
        publishPartitionEvents(fqtn, partitionEvents);
      } else {
        log.info("No partition events to publish for table: {} (unpartitioned or none)", fqtn);
      }

      List<CommitEventTablePartitionStats> partitionStats =
          ops.collectCommitEventTablePartitionStats(fqtn);
      if (partitionStats != null && !partitionStats.isEmpty()) {
        publishPartitionStats(fqtn, partitionStats);
      } else {
        log.info("No partition stats to publish for table: {} (unpartitioned or none)", fqtn);
      }
    }
  }

  /**
   * Per-table inputs for one operation row inside a bin. {@code operationId} and {@code tableUuid}
   * are exposed as {@link Optional} because the legacy scheduler path leaves them unset (no
   * optimizer-service context); the optimizer-service path always populates them.
   */
  @lombok.AllArgsConstructor
  @lombok.Builder
  @lombok.ToString
  public static class BatchEntry {
    @lombok.Getter private final String fqtn;
    private final String operationId;
    private final String tableUuid;
    @lombok.Getter private final String databaseName;
    @lombok.Getter private final String tableName;

    public Optional<String> getOperationId() {
      return Optional.ofNullable(operationId);
    }

    public Optional<String> getTableUuid() {
      return Optional.ofNullable(tableUuid);
    }
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Collections.singletonList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  public static BatchedTableStatsCollectionSparkApp createApp(
      String[] args, OtelEmitter otelEmitter) {
    List<Option> extraOptions =
        Arrays.asList(
            valueOpt("tableNames", "Comma-separated list of fully-qualified table names"),
            valueOpt("operationIds", "Comma-separated operation UUIDs, parallel to tableNames"),
            valueOpt("tableUuids", "Comma-separated table UUIDs, parallel to tableNames"),
            valueOpt("resultsEndpoint", "Base URL of the Optimizer Service"),
            valueOpt("driverParallelism", "Worker threads in this batch (default 1)"));

    CommandLine cmdLine = createCommandLine(args, extraOptions);

    List<BatchEntry> entries =
        buildEntries(
            cmdLine.getOptionValue("tableNames"),
            cmdLine.getOptionValue("operationIds"),
            cmdLine.getOptionValue("tableUuids"));

    return new BatchedTableStatsCollectionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        otelEmitter,
        entries,
        requireOption(cmdLine, "resultsEndpoint"),
        Integer.parseInt(cmdLine.getOptionValue("driverParallelism", "1")));
  }

  static List<BatchEntry> buildEntries(String tableNames, String operationIds, String tableUuids) {
    if (tableNames == null || tableNames.isEmpty()) {
      throw new IllegalArgumentException("--tableNames is required and must be non-empty");
    }
    String[] tables = tableNames.split(",");
    if (tables.length > AppConstants.STATS_MAX_BATCH_SIZE) {
      throw new IllegalArgumentException(
          String.format(
              "Batch size %d exceeds STATS_MAX_BATCH_SIZE=%d; reduce max-tables-per-bin on the scheduler",
              tables.length, AppConstants.STATS_MAX_BATCH_SIZE));
    }
    String[] ops = StringUtils.isBlank(operationIds) ? null : operationIds.split(",");
    String[] uuids = StringUtils.isBlank(tableUuids) ? null : tableUuids.split(",");
    if (ops != null && ops.length != tables.length) {
      throw new IllegalArgumentException(
          String.format(
              "Parallel-list length mismatch: tableNames=%d operationIds=%d",
              tables.length, ops.length));
    }
    if (uuids != null && uuids.length != tables.length) {
      throw new IllegalArgumentException(
          String.format(
              "Parallel-list length mismatch: tableNames=%d tableUuids=%d",
              tables.length, uuids.length));
    }
    List<BatchEntry> entries = new ArrayList<>(tables.length);
    for (int i = 0; i < tables.length; i++) {
      String fqtn = tables[i].trim();
      String[] dbAndTable = fqtn.split("\\.", 2);
      if (dbAndTable.length != 2 || dbAndTable[0].isEmpty() || dbAndTable[1].isEmpty()) {
        throw new IllegalArgumentException(
            "tableNames entries must be fully-qualified (db.table): " + fqtn);
      }
      entries.add(
          BatchEntry.builder()
              .fqtn(fqtn)
              .operationId(ops == null ? null : ops[i].trim())
              .tableUuid(uuids == null ? null : uuids[i].trim())
              .databaseName(dbAndTable[0])
              .tableName(dbAndTable[1])
              .build());
    }
    return entries;
  }

  private static String requireOption(CommandLine cmdLine, String name) {
    String value = cmdLine.getOptionValue(name);
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("--" + name + " is required");
    }
    return value;
  }

  /** Long-only CLI option carrying a value (read with {@code cmdLine.getOptionValue(name)}). */
  private static Option valueOpt(String name, String description) {
    return new Option(null, name, true, description);
  }

  /** Visible for tests. */
  List<BatchEntry> getEntries() {
    return Collections.unmodifiableList(entries);
  }

  /** Visible for tests. */
  int getDriverParallelism() {
    return driverParallelism;
  }
}
