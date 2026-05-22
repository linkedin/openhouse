package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.exception.TableValidationException;
import com.linkedin.openhouse.jobs.spark.optimizer.OperationUpdateRequest;
import com.linkedin.openhouse.jobs.spark.optimizer.OptimizerServiceClient;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.jobs.util.TableStateValidator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

  private static final String OPERATION_TYPE = "ORPHAN_FILES_DELETION";
  private static final String STATUS_SUCCESS = "SUCCESS";
  private static final String STATUS_FAILED = "FAILED";
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

    int successCount;
    try (OptimizerServiceClient client = newOptimizerClient()) {
      successCount = runBatch(ops, client);
    }

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
      List<Future<Boolean>> futures = new ArrayList<>(entries.size());
      for (BatchEntry entry : entries) {
        futures.add(pool.submit(new TableWorker(ops, entry, client)));
      }
      int successes = 0;
      for (int i = 0; i < futures.size(); i++) {
        try {
          if (Boolean.TRUE.equals(futures.get(i).get())) {
            successes++;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.error("Interrupted while waiting on table {}", entries.get(i).getFqtn(), e);
        } catch (ExecutionException e) {
          // Per-table workers swallow their own failures and report FAILED upstream; an
          // ExecutionException here means the worker itself threw, which we treat as a failed
          // operation but otherwise let the batch continue.
          log.error("Worker threw for table {}", entries.get(i).getFqtn(), e.getCause());
        }
      }
      return successes;
    } finally {
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
  }

  protected OptimizerServiceClient newOptimizerClient() {
    return new OptimizerServiceClient(resultsEndpoint);
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
      boolean success = false;
      try {
        log.info("OFD start: fqtn={} operationId={}", fqtn, entry.getOperationId());
        Table table = ops.getTable(fqtn);
        long effectiveTtlSeconds = resolveTtlSeconds(table);
        long olderThanTimestampMillis =
            System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(effectiveTtlSeconds);
        boolean backupEnabled =
            Boolean.parseBoolean(
                table.properties().getOrDefault(AppConstants.BACKUP_ENABLED_KEY, "false"));
        DeleteOrphanFiles.Result result =
            ops.deleteOrphanFiles(
                table,
                olderThanTimestampMillis,
                backupEnabled,
                backupDir,
                concurrentDeletes,
                streamResults,
                maxOrphanFileSampleSize);
        int orphanCount = countOrphans(result);
        otelEmitter.count(
            METRICS_SCOPE,
            AppConstants.ORPHAN_FILE_COUNT,
            orphanCount,
            Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), fqtn));
        validate(fqtn);
        success = true;
        log.info("OFD success: fqtn={} orphansDetected={}", fqtn, orphanCount);
      } catch (TableValidationException e) {
        log.error("Post-job validation failed for fqtn={}", fqtn, e);
      } catch (Throwable t) {
        log.error("OFD failed: fqtn={} operationId={}", fqtn, entry.getOperationId(), t);
      } finally {
        reportResult(success);
      }
      return success;
    }

    private void validate(String fqtn) {
      try {
        TableStateValidator.run(ops.spark(), fqtn);
      } catch (TableValidationException e) {
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

    private void reportResult(boolean success) {
      OperationUpdateRequest body =
          OperationUpdateRequest.builder()
              .operationId(entry.getOperationId())
              .status(success ? STATUS_SUCCESS : STATUS_FAILED)
              .tableUuid(entry.getTableUuid())
              .databaseName(entry.getDatabaseName())
              .tableName(entry.getTableName())
              .operationType(OPERATION_TYPE)
              .build();
      try {
        client.updateOperation(body);
      } catch (IOException e) {
        log.error(
            "Failed to report operation result; row will stay SCHEDULED until stale-timeout: operationId={} fqtn={}",
            entry.getOperationId(),
            entry.getFqtn(),
            e);
      }
    }

    private long resolveTtlSeconds(Table table) {
      long resolved = ttlSeconds;
      boolean oneDayTtlEnabled =
          Boolean.parseBoolean(
              table.properties().getOrDefault(AppConstants.OFD_ONE_DAY_TTL_ENABLED_KEY, "false"));
      if (oneDayTtlEnabled) {
        resolved = TimeUnit.DAYS.toSeconds(1);
      }
      String tableType =
          table
              .properties()
              .getOrDefault(AppConstants.OPENHOUSE_TABLE_TYPE_KEY, AppConstants.TABLE_TYPE_PRIMARY);
      if (AppConstants.TABLE_TYPE_REPLICA.equals(tableType)) {
        long days = Duration.ofSeconds(resolved).toDays();
        if (days < DEFAULT_MIN_OFD_TTL_IN_DAYS) {
          resolved = TimeUnit.DAYS.toSeconds(DEFAULT_MIN_OFD_TTL_IN_DAYS);
        }
      }
      return resolved;
    }

    private int countOrphans(DeleteOrphanFiles.Result result) {
      int count = 0;
      for (String unused : result.orphanFileLocations()) {
        count++;
      }
      return count;
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
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(
        new Option(
            null, "tableNames", true, "Comma-separated list of fully-qualified table names"));
    extraOptions.add(
        new Option(
            null, "operationIds", true, "Comma-separated operation UUIDs, parallel to tableNames"));
    extraOptions.add(
        new Option(
            null, "tableUuids", true, "Comma-separated table UUIDs, parallel to tableNames"));
    extraOptions.add(
        new Option(null, "resultsEndpoint", true, "Base URL of the Optimizer Service"));
    extraOptions.add(
        new Option(null, "driverParallelism", true, "Worker threads in this batch (default 1)"));
    extraOptions.add(
        new Option("tr", "trashDir", true, "Orphan files staging dir before deletion"));
    extraOptions.add(
        new Option(
            "r",
            "ttl",
            true,
            "How old files should be to be considered orphaned in seconds, minimum 1d is enforced"));
    extraOptions.add(new Option("b", "backupDir", true, "Backup directory for deleted data"));
    extraOptions.add(
        new Option("c", "concurrentDeletes", true, "Number of concurrent deletes per table"));
    extraOptions.add(
        new Option(
            null, "streamResults", false, "Stream orphan file deletions instead of collecting"));
    extraOptions.add(
        new Option(null, "maxOrphanFileSampleSize", true, "Max orphan file sample paths returned"));

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

  /** Visible for tests. */
  List<BatchEntry> getEntries() {
    return Collections.unmodifiableList(entries);
  }

  /** Visible for tests. */
  int getDriverParallelism() {
    return driverParallelism;
  }
}
