package com.linkedin.openhouse.jobs.spark;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.spark.state.StateManager;
import com.linkedin.openhouse.jobs.util.AppConstants;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iceberg.Table;

/**
 * Runs orphan file deletion for a batch of tables in one Spark session.
 *
 * <p>Spinning up a Spark session per table is expensive. This app amortizes that cost by processing
 * many tables in a single job, using a driver-side thread pool so each table's deletion runs
 * concurrently without competing for executors.
 *
 * <p>When {@code --resultsEndpoint} is supplied, each table's outcome is POSTed to the optimizer
 * service's complete-operation endpoint as it completes, letting the service track per-table status
 * independently of the overall job.
 */
@Slf4j
public class BatchedOrphanFilesDeletionSparkApp extends BaseSparkApp {
  private static final int DEFAULT_MAX_ORPHAN_FILE_SAMPLE_SIZE = 20000;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final List<String> tableNames;
  private final int parallelism;
  private final long ttlSeconds;
  private final String backupDir;
  private final int concurrentDeletes;
  private final boolean streamResults;
  private final int maxOrphanFileSampleSize;
  private final List<String> operationIds;
  private final String resultsEndpoint;

  public BatchedOrphanFilesDeletionSparkApp(
      String jobId,
      StateManager stateManager,
      List<String> tableNames,
      int parallelism,
      long ttlSeconds,
      OtelEmitter otelEmitter,
      String backupDir,
      int concurrentDeletes,
      boolean streamResults,
      int maxOrphanFileSampleSize,
      List<String> operationIds,
      String resultsEndpoint) {
    super(jobId, stateManager, otelEmitter);
    this.tableNames = tableNames;
    this.parallelism = parallelism;
    this.ttlSeconds = ttlSeconds;
    this.backupDir = backupDir;
    this.concurrentDeletes = concurrentDeletes;
    this.streamResults = streamResults;
    this.maxOrphanFileSampleSize = maxOrphanFileSampleSize;
    this.operationIds = operationIds;
    this.resultsEndpoint = resultsEndpoint;
  }

  @Override
  protected void runInner(Operations ops) throws Exception {
    if (resultsEndpoint != null && operationIds.size() != tableNames.size()) {
      throw new IllegalArgumentException(
          "operationIds count ("
              + operationIds.size()
              + ") must equal tableNames count ("
              + tableNames.size()
              + ") when resultsEndpoint is provided");
    }

    Map<String, String> tableToOperationId = new HashMap<>();
    if (resultsEndpoint != null) {
      for (int i = 0; i < tableNames.size(); i++) {
        tableToOperationId.put(tableNames.get(i), operationIds.get(i));
      }
    }

    long olderThanTimestampMillis =
        System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(ttlSeconds);

    log.info(
        "Starting batched orphan files deletion for {} tables with parallelism {}",
        tableNames.size(),
        parallelism);

    int poolSize = Math.min(parallelism, Math.max(1, tableNames.size()));
    ExecutorService executor = Executors.newFixedThreadPool(poolSize);
    List<Future<OrphanDeletionResult>> futures = new ArrayList<>();

    for (String tableName : tableNames) {
      futures.add(
          executor.submit(
              () -> {
                long startTime = System.currentTimeMillis();
                try {
                  Table table = ops.getTable(tableName);
                  boolean backupEnabled =
                      Boolean.parseBoolean(
                          table
                              .properties()
                              .getOrDefault(AppConstants.BACKUP_ENABLED_KEY, "false"));

                  log.info("Processing orphan files for table: {}", tableName);
                  Operations.OrphanFilesResult result =
                      ops.deleteOrphanFilesWithMetrics(
                          table,
                          olderThanTimestampMillis,
                          backupEnabled,
                          backupDir,
                          concurrentDeletes,
                          streamResults,
                          maxOrphanFileSampleSize);

                  List<String> orphanFiles =
                      Lists.newArrayList(result.orphanFileLocations().iterator());
                  long durationMs = System.currentTimeMillis() - startTime;

                  log.info(
                      "Successfully processed table {}: {} orphan files deleted in {}ms",
                      tableName,
                      orphanFiles.size(),
                      durationMs);

                  return OrphanDeletionResult.success(
                      tableName, orphanFiles.size(), result.getBytesDeleted(), durationMs);

                } catch (Exception e) {
                  long durationMs = System.currentTimeMillis() - startTime;
                  log.error("Failed to process table: {}", tableName, e);
                  return OrphanDeletionResult.failure(tableName, e, durationMs);
                }
              }));
    }

    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    List<OrphanDeletionResult> allResults = new ArrayList<>();
    for (Future<OrphanDeletionResult> future : futures) {
      try {
        allResults.add(future.get());
      } catch (ExecutionException e) {
        throw new RuntimeException("Unexpected exception in table processing", e.getCause());
      }
    }

    reportResults(allResults, tableToOperationId);
  }

  private void reportResults(
      List<OrphanDeletionResult> results, Map<String, String> tableToOperationId) throws Exception {
    OkHttpClient client =
        resultsEndpoint != null
            ? new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build()
            : null;

    int failureCount = 0;
    for (OrphanDeletionResult result : results) {
      if (result.isSuccess()) {
        otelEmitter.count(
            METRICS_SCOPE,
            AppConstants.ORPHAN_FILE_COUNT,
            result.getOrphanFilesDeleted(),
            Attributes.of(AttributeKey.stringKey(AppConstants.TABLE_NAME), result.getTableName()));
      } else {
        failureCount++;
      }

      if (client != null) {
        String opId = tableToOperationId.get(result.getTableName());
        if (opId != null) {
          completeOperation(client, opId, result);
        }
      }
    }

    if (client == null && failureCount > 0) {
      throw new RuntimeException(failureCount + " table(s) failed in batch");
    }
    if (client != null && failureCount == results.size()) {
      throw new RuntimeException("All tables failed in batch");
    }
  }

  private void completeOperation(OkHttpClient client, String id, OrphanDeletionResult result)
      throws Exception {
    OperationResult opResult =
        result.isSuccess()
            ? OperationResult.success(result.getOrphanFilesDeleted(), result.getBytesDeleted())
            : OperationResult.failure(result.getErrorMessage(), result.getErrorType());

    String json = OBJECT_MAPPER.writeValueAsString(opResult);
    RequestBody body = RequestBody.create(json, MediaType.parse("application/json; charset=utf-8"));
    Request request =
        new Request.Builder().url(resultsEndpoint + "/" + id + "/complete").post(body).build();
    try (Response response = client.newCall(request).execute()) {
      int code = response.code();
      if (code < 200 || code >= 300) {
        throw new RuntimeException("POST operation/" + id + "/complete returned HTTP " + code);
      }
    }
  }

  /** Result of orphan deletion for a single table. */
  @Value
  @Builder
  public static class OrphanDeletionResult implements Serializable {
    String tableName;
    boolean success;
    int orphanFilesDeleted;
    long bytesDeleted;
    long durationMs;
    String errorMessage;
    String errorType;

    public static OrphanDeletionResult success(
        String tableName, int orphanFileCount, long bytesDeleted, long durationMs) {
      return OrphanDeletionResult.builder()
          .tableName(tableName)
          .success(true)
          .orphanFilesDeleted(orphanFileCount)
          .bytesDeleted(bytesDeleted)
          .durationMs(durationMs)
          .build();
    }

    public static OrphanDeletionResult failure(String tableName, Throwable error, long durationMs) {
      return OrphanDeletionResult.builder()
          .tableName(tableName)
          .success(false)
          .orphanFilesDeleted(0)
          .bytesDeleted(0)
          .durationMs(durationMs)
          .errorMessage(error.getMessage())
          .errorType(error.getClass().getSimpleName())
          .build();
    }
  }

  /** POST payload sent to the optimizer service's complete-operation endpoint. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  static class OperationResult {
    public final String status;
    public final Integer orphanFilesDeleted;
    public final Long orphanBytesDeleted;
    public final String errorMessage;
    public final String errorType;

    private OperationResult(
        String status,
        Integer orphanFilesDeleted,
        Long orphanBytesDeleted,
        String errorMessage,
        String errorType) {
      this.status = status;
      this.orphanFilesDeleted = orphanFilesDeleted;
      this.orphanBytesDeleted = orphanBytesDeleted;
      this.errorMessage = errorMessage;
      this.errorType = errorType;
    }

    static OperationResult success(int orphanFilesDeleted, long orphanBytesDeleted) {
      return new OperationResult("SUCCESS", orphanFilesDeleted, orphanBytesDeleted, null, null);
    }

    static OperationResult failure(String errorMessage, String errorType) {
      return new OperationResult("FAILED", null, null, errorMessage, errorType);
    }
  }

  public static void main(String[] args) {
    OtelEmitter otelEmitter =
        new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));
    createApp(args, otelEmitter).run();
  }

  public static BatchedOrphanFilesDeletionSparkApp createApp(
      String[] args, OtelEmitter otelEmitter) {
    List<Option> extraOptions = new ArrayList<>();
    extraOptions.add(
        new Option("tn", "tableNames", true, "Comma-separated fully-qualified table names"));
    extraOptions.add(new Option("p", "parallelism", true, "Number of parallel table processes"));
    extraOptions.add(
        new Option(
            "r",
            "ttl",
            true,
            "How old files should be to be considered orphaned in seconds, minimum 1d is"
                + " enforced"));
    extraOptions.add(new Option("b", "backupDir", true, "Backup directory for deleted data"));
    extraOptions.add(new Option("c", "concurrentDeletes", true, "Number of concurrent deletes"));
    extraOptions.add(
        new Option(
            null,
            "streamResults",
            false,
            "Stream orphan file deletions instead of collecting all paths into driver memory"));
    extraOptions.add(
        new Option(
            null,
            "maxOrphanFileSampleSize",
            true,
            "Maximum number of orphan file paths to return in the result when streaming"));
    extraOptions.add(new Option("oi", "operationIds", true, "Comma-separated operation IDs"));
    extraOptions.add(new Option("re", "resultsEndpoint", true, "Base URL for per-table PATCH"));

    CommandLine cmdLine = createCommandLine(args, extraOptions);

    String tableNamesStr = cmdLine.getOptionValue("tableNames");
    List<String> tableNames =
        tableNamesStr != null ? Arrays.asList(tableNamesStr.split(",")) : new ArrayList<>();

    String idsStr = cmdLine.getOptionValue("operationIds");
    List<String> operationIds =
        idsStr != null ? Arrays.asList(idsStr.split(",")) : Collections.emptyList();
    String resultsEndpoint = cmdLine.getOptionValue("resultsEndpoint");

    long rawTtl = NumberUtils.toLong(cmdLine.getOptionValue("ttl"), TimeUnit.DAYS.toSeconds(7));
    // TTL=0 bypasses the minimum-age guard (for tests that seed orphan files and need
    // them deleted immediately). Any other explicit value is clamped to the 1-day minimum.
    long ttlSeconds = rawTtl == 0 ? 0 : Math.max(rawTtl, TimeUnit.DAYS.toSeconds(1));

    return new BatchedOrphanFilesDeletionSparkApp(
        getJobId(cmdLine),
        createStateManager(cmdLine, otelEmitter),
        tableNames,
        Integer.parseInt(cmdLine.getOptionValue("parallelism", "10")),
        ttlSeconds,
        otelEmitter,
        cmdLine.getOptionValue("backupDir", ".backup"),
        Integer.parseInt(cmdLine.getOptionValue("concurrentDeletes", "10")),
        cmdLine.hasOption("streamResults"),
        Integer.parseInt(
            cmdLine.getOptionValue(
                "maxOrphanFileSampleSize", String.valueOf(DEFAULT_MAX_ORPHAN_FILE_SAMPLE_SIZE))),
        operationIds,
        resultsEndpoint);
  }
}
