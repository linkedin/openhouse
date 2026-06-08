package com.linkedin.openhouse.jobs.spark;

import com.google.common.collect.Iterables;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.exception.TableValidationException;
import com.linkedin.openhouse.jobs.spark.optimizer.OptimizerServiceClient;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.jobs.util.TableStateValidator;
import com.linkedin.openhouse.optimizer.client.model.UpdateOperationRequest;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;

/**
 * Batched orphan-files-deletion Spark app. One Spark job processes a list of {@code (table,
 * operationId)} pairs that the optimizer scheduler bin-packed into a single batch. Each table is
 * handled by a worker thread; per-table failures are caught and reported back independently — the
 * job continues for the remaining tables and exits 0 if at least one table succeeds.
 *
 * <p>This is the multi-table counterpart of {@link OrphanFilesDeletionSparkApp}. The single-table
 * app remains the deployment unit when bin size is 1, and stays the canonical reference for the
 * actual deletion logic.
 *
 * <p>Example invocation:
 *
 * <pre>{@code
 * com.linkedin.openhouse.jobs.spark.BatchedOrphanFilesDeletionSparkApp \
 *   --tableNames db.t1,db.t2,db.t3 \
 *   --operationIds op-uuid-1,op-uuid-2,op-uuid-3 \
 *   --tableUuids tab-uuid-1,tab-uuid-2,tab-uuid-3 \
 *   --resultsEndpoint http://optimizer.svc:8080 \
 *   --driverParallelism 4
 * }</pre>
 */
@Slf4j
public class BatchedOrphanFilesDeletionSparkApp extends BaseSparkApp {

  private static final int DEFAULT_MAX_ORPHAN_FILE_SAMPLE_SIZE = 20000;
  private static final int DEFAULT_MIN_OFD_TTL_IN_DAYS = 3;

  private final List<BatchEntry> entries;
  private final String resultsEndpoint;
  private final int driverParallelism;
  private final long ttlSeconds;
  private final String backupDir;
  private final int concurrentDeletes;
  private final boolean streamResults;
  private final int maxOrphanFileSampleSize;

  public BatchedOrphanFilesDeletionSparkApp(
      String jobId,
      StateManager stateManager,
      OtelEmitter otelEmitter,
      List<BatchEntry> entries,
      String resultsEndpoint,
      int driverParallelism,
      long ttlSeconds,
      String backupDir,
      int concurrentDeletes,
      boolean streamResults,
      int maxOrphanFileSampleSize) {
    super(jobId, stateManager, otelEmitter);
    this.entries = entries;
    this.resultsEndpoint = resultsEndpoint;
    this.driverParallelism = Math.max(1, driverParallelism);
    this.ttlSeconds = ttlSeconds;
    this.backupDir = backupDir;
    this.concurrentDeletes = concurrentDeletes;
    this.streamResults = streamResults;
    this.maxOrphanFileSampleSize = maxOrphanFileSampleSize;
  }

  @Override
  protected void runInner(Operations ops) {
    log.info(
        "Batched OFD start: entries={} driverParallelism={} resultsEndpoint={}",
        entries.size(),
        driverParallelism,
        resultsEndpoint);

    if (entries.isEmpty()) {
      log.warn("Batched OFD invoked with no entries; nothing to do");
      return;
    }

    OptimizerServiceClient client = newOptimizerClient();
    int successCount = runBatch(ops, client);

    int failureCount = entries.size() - successCount;
    log.info(
        "Batched OFD finished: total={} success={} failed={}",
        entries.size(),
        successCount,
        failureCount);

    if (successCount == 0) {
      throw new RuntimeException(
          String.format("All %d operations in batch failed", entries.size()));
    }
  }

  private int runBatch(Operations ops, OptimizerServiceClient client) {
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

  private int awaitOne(BatchEntry entry, Future<Boolean> future, OptimizerServiceClient client) {
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

  protected OptimizerServiceClient newOptimizerClient() {
    return new OptimizerServiceClient(resultsEndpoint);
  }

  /**
   * POST the per-operation outcome to the Optimizer Service via the generated client. The wrapper
   * returns {@link java.util.Optional#empty()} when the call exhausts retries — we log + count and
   * leave the operation row at SCHEDULED so the Analyzer's stale-timeout can re-queue it.
   *
   * <p>{@code status} is passed in as a {@link UpdateOperationRequest.StatusEnum} rather than a
   * boolean so the caller's intent is unambiguous and new terminal states (e.g. CANCELED) can be
   * plumbed in without changing the signature.
   */
  private void reportResult(
      BatchEntry entry, UpdateOperationRequest.StatusEnum status, OptimizerServiceClient client) {
    UpdateOperationRequest body =
        new UpdateOperationRequest()
            .operationId(entry.getOperationId())
            .status(status)
            .tableUuid(entry.getTableUuid())
            .databaseName(entry.getDatabaseName())
            .tableName(entry.getTableName())
            .operationType(UpdateOperationRequest.OperationTypeEnum.ORPHAN_FILES_DELETION);
    if (!client.updateOperation(entry.getOperationId(), body).isPresent()) {
      log.error(
          "Failed to report operation result after retries; row will stay SCHEDULED until stale-timeout: operationId={} fqtn={}",
          entry.getOperationId(),
          entry.getFqtn());
      otelEmitter.count(
          METRICS_SCOPE,
          "optimizer_update_failed",
          1,
          Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), entry.getFqtn()));
    }
  }

  /** One unit of work in a batched OFD job. */
  private final class TableWorker implements Callable<Boolean> {
    private final Operations ops;
    private final BatchEntry entry;
    private final OptimizerServiceClient client;

    TableWorker(Operations ops, BatchEntry entry, OptimizerServiceClient client) {
      this.ops = ops;
      this.entry = entry;
      this.client = client;
    }

    @Override
    public Boolean call() {
      String fqtn = entry.getFqtn();
      UpdateOperationRequest.StatusEnum status = UpdateOperationRequest.StatusEnum.FAILED;
      try {
        log.info("OFD start: fqtn={} operationId={}", fqtn, entry.getOperationId());
        Table table = ops.getTable(fqtn);
        long olderThanTimestampMillis =
            System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(resolveTtlSeconds(table));
        DeleteOrphanFiles.Result result =
            ops.deleteOrphanFiles(
                table,
                olderThanTimestampMillis,
                Boolean.parseBoolean(
                    table.properties().getOrDefault(AppConstants.BACKUP_ENABLED_KEY, "false")),
                backupDir,
                concurrentDeletes,
                streamResults,
                maxOrphanFileSampleSize);
        // Count via iteration rather than materializing the full path list: a table with millions
        // of orphan files would otherwise OOM the driver, and that risk multiplies with
        // driverParallelism workers running concurrently.
        int orphanCount = Iterables.size(result.orphanFileLocations());
        otelEmitter.count(
            METRICS_SCOPE,
            AppConstants.ORPHAN_FILE_COUNT,
            orphanCount,
            Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), fqtn));
        validate(fqtn);
        status = UpdateOperationRequest.StatusEnum.SUCCESS;
        log.info("OFD success: fqtn={} orphansDetected={}", fqtn, orphanCount);
      } catch (Throwable t) {
        log.error("OFD failed: fqtn={} operationId={}", fqtn, entry.getOperationId(), t);
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
     * Re-runs {@link TableStateValidator} — the same post-job consistency check the single-table
     * {@link OrphanFilesDeletionSparkApp} uses — to confirm the table's manifests and metadata are
     * intact after deletion. A failure here is treated as a failed operation: it's logged, counted,
     * and re-thrown so the outer {@link #call()} marks {@code success=false}.
     */
    private void validate(String fqtn) {
      try {
        TableStateValidator.run(ops.spark(), fqtn);
      } catch (TableValidationException e) {
        log.error("Post-job validation failed: fqtn={}", fqtn, e);
        otelEmitter.count(
            METRICS_SCOPE,
            "post_run_validation_error",
            1,
            Attributes.of(
                AttributeKey.stringKey(AppConstants.TABLE_NAME),
                fqtn,
                AttributeKey.stringKey(AppConstants.JOB_NAME),
                BatchedOrphanFilesDeletionSparkApp.class.getSimpleName()));
        throw e;
      }
    }

    private long resolveTtlSeconds(Table table) {
      long resolved = ttlSeconds;
      if (Boolean.parseBoolean(
          table.properties().getOrDefault(AppConstants.OFD_ONE_DAY_TTL_ENABLED_KEY, "false"))) {
        resolved = TimeUnit.DAYS.toSeconds(1);
      }
      String tableType =
          table
              .properties()
              .getOrDefault(AppConstants.OPENHOUSE_TABLE_TYPE_KEY, AppConstants.TABLE_TYPE_PRIMARY);
      if (AppConstants.TABLE_TYPE_REPLICA.equals(tableType)
          && Duration.ofSeconds(resolved).toDays() < DEFAULT_MIN_OFD_TTL_IN_DAYS) {
        resolved = TimeUnit.DAYS.toSeconds(DEFAULT_MIN_OFD_TTL_IN_DAYS);
      }
      return resolved;
    }
  }

  /** Per-table inputs for one operation row inside a bin. */
  @lombok.AllArgsConstructor
  @lombok.Builder
  @lombok.Getter
  @lombok.ToString
  public static class BatchEntry {
    private final String fqtn;
    private final String operationId;
    private final String tableUuid;
    private final String databaseName;
    private final String tableName;
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Collections.singletonList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  public static BatchedOrphanFilesDeletionSparkApp createApp(
      String[] args, OtelEmitter otelEmitter) {
    List<Option> extraOptions =
        Arrays.asList(
            valueOpt("tableNames", "Comma-separated list of fully-qualified table names"),
            valueOpt("operationIds", "Comma-separated operation UUIDs, parallel to tableNames"),
            valueOpt("tableUuids", "Comma-separated table UUIDs, parallel to tableNames"),
            valueOpt("resultsEndpoint", "Base URL of the Optimizer Service"),
            valueOpt("driverParallelism", "Worker threads in this batch (default 1)"),
            valueOpt("trashDir", "tr", "Orphan files staging dir before deletion"),
            valueOpt(
                "ttl",
                "r",
                "How old files should be to be considered orphaned in seconds, minimum 1d is enforced"),
            valueOpt("backupDir", "b", "Backup directory for deleted data"),
            valueOpt("concurrentDeletes", "c", "Number of concurrent deletes per table"),
            flagOpt("streamResults", "Stream orphan file deletions instead of collecting"),
            valueOpt("maxOrphanFileSampleSize", "Max orphan file sample paths returned"));

    CommandLine cmdLine = createCommandLine(args, extraOptions);

    List<BatchEntry> entries =
        buildEntries(
            cmdLine.getOptionValue("tableNames"),
            cmdLine.getOptionValue("operationIds"),
            cmdLine.getOptionValue("tableUuids"));

    return new BatchedOrphanFilesDeletionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        otelEmitter,
        entries,
        requireOption(cmdLine, "resultsEndpoint"),
        Integer.parseInt(cmdLine.getOptionValue("driverParallelism", "1")),
        Math.max(
            NumberUtils.toLong(cmdLine.getOptionValue("ttl"), TimeUnit.DAYS.toSeconds(7)),
            TimeUnit.DAYS.toSeconds(1)),
        cmdLine.getOptionValue("backupDir", ".backup"),
        Integer.parseInt(cmdLine.getOptionValue("concurrentDeletes", "10")),
        cmdLine.hasOption("streamResults"),
        Integer.parseInt(
            cmdLine.getOptionValue(
                "maxOrphanFileSampleSize", String.valueOf(DEFAULT_MAX_ORPHAN_FILE_SAMPLE_SIZE))));
  }

  static List<BatchEntry> buildEntries(String tableNames, String operationIds, String tableUuids) {
    if (tableNames == null
        || operationIds == null
        || tableUuids == null
        || tableNames.isEmpty()
        || operationIds.isEmpty()
        || tableUuids.isEmpty()) {
      throw new IllegalArgumentException(
          "--tableNames, --operationIds, and --tableUuids are all required and must be non-empty");
    }
    String[] tables = tableNames.split(",");
    String[] ops = operationIds.split(",");
    String[] uuids = tableUuids.split(",");
    if (tables.length != ops.length || tables.length != uuids.length) {
      throw new IllegalArgumentException(
          String.format(
              "Parallel-list length mismatch: tableNames=%d operationIds=%d tableUuids=%d",
              tables.length, ops.length, uuids.length));
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
              .operationId(ops[i].trim())
              .tableUuid(uuids[i].trim())
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

  /** Aliased CLI option carrying a value. {@code shortOpt} is the legacy single-letter alias. */
  private static Option valueOpt(String name, String shortOpt, String description) {
    return new Option(shortOpt, name, true, description);
  }

  /** Long-only boolean CLI flag (read with {@code cmdLine.hasOption(name)}). */
  private static Option flagOpt(String name, String description) {
    return new Option(null, name, false, description);
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
