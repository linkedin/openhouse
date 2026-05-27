package com.linkedin.openhouse.tables.services.optimizer;

import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.tables.model.TableDto;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetrySpec;

/**
 * Pushes a stats record to the optimizer's {@code PUT /v1/optimizer/stats/{tableUuid}} endpoint
 * after a successful Iceberg snapshot commit. Fire-and-forget — the {@link #report} call returns
 * immediately on the commit thread; the HTTP exchange runs on the WebClient's Netty event loop.
 *
 * <p>Errors are recorded as Micrometer metrics and logged at {@code warn} level, but never
 * propagated. A push failure must not break the commit.
 *
 * <p>Timeout / retry shape (from {@link OptimizerStatsProperties}):
 *
 * <ul>
 *   <li>{@code perAttemptTimeoutMs} bounds each HTTP attempt (1000 ms default).
 *   <li>{@code maxAttempts} caps the total number of attempts (3 default — one initial + two
 *       retries). Retries only fire on retryable errors (network, 5xx, 408, 429, timeout).
 *   <li>{@code totalTimeoutMs} is the outer wall-clock ceiling (2000 ms default). Hard cancel.
 * </ul>
 *
 * <p>The bean is only wired when {@code optimizer.stats.enabled=true}.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "optimizer.stats", name = "enabled", havingValue = "true")
public class OptimizerStatsClient {

  /** Per-call URL path. */
  static final String PATH_TEMPLATE = "/v1/optimizer/stats/{tableUuid}";

  /** Table-property key that opts a table in for the post-commit stats push. */
  static final String OPT_IN_PROPERTY = "maintenance.optimizer.stats.enabled";

  /** Iceberg snapshot-summary keys we read. All values are decimal-string longs. */
  static final String SUMMARY_TOTAL_DATA_FILES = "total-data-files";

  static final String SUMMARY_TOTAL_FILES_SIZE = "total-files-size";
  static final String SUMMARY_ADDED_DATA_FILES = "added-data-files";
  static final String SUMMARY_DELETED_DATA_FILES = "deleted-data-files";
  static final String SUMMARY_ADDED_FILES_SIZE = "added-files-size";
  static final String SUMMARY_REMOVED_FILES_SIZE = "removed-files-size";

  private final WebClient webClient;
  private final MeterRegistry meterRegistry;
  private final OptimizerStatsProperties properties;

  public OptimizerStatsClient(
      @Qualifier("optimizerStatsWebClient") WebClient webClient,
      MeterRegistry meterRegistry,
      OptimizerStatsProperties properties) {
    this.webClient = webClient;
    this.meterRegistry = meterRegistry;
    this.properties = properties;
  }

  /**
   * Build and dispatch a stats push for the just-committed table. Returns immediately; the HTTP
   * call runs in the background. {@code snapshotSummary} is the in-memory {@code
   * Snapshot.summary()} map from the latest snapshot (no HDFS round-trip).
   *
   * <p>The call is skipped (and a {@code skipped} counter is emitted) when:
   *
   * <ul>
   *   <li>The table is not opted in via {@link #OPT_IN_PROPERTY}.
   *   <li>{@code snapshotSummary} is null or empty (no committed snapshot yet — e.g. CREATE TABLE
   *       with no rows).
   * </ul>
   */
  public void report(TableDto saved, Map<String, String> snapshotSummary) {
    reportAsync(saved, snapshotSummary).subscribe();
  }

  /**
   * Same as {@link #report} but returns the underlying {@link Mono} so callers (notably tests) can
   * block on completion. Production callers use {@link #report}.
   */
  Mono<Void> reportAsync(TableDto saved, Map<String, String> snapshotSummary) {
    if (!isOptedIn(saved)) {
      meterRegistry
          .counter(MetricsConstant.OPTIMIZER_STATS_SKIPPED, "reason", "opt_out")
          .increment();
      return Mono.empty();
    }
    if (snapshotSummary == null || snapshotSummary.isEmpty()) {
      meterRegistry
          .counter(MetricsConstant.OPTIMIZER_STATS_SKIPPED, "reason", "no_snapshot")
          .increment();
      return Mono.empty();
    }

    OptimizerStatsRequest body = buildRequest(saved, snapshotSummary);
    String tableUuid = saved.getTableUUID();
    Timer.Sample sample = Timer.start(meterRegistry);

    RetrySpec retrySpec =
        Retry.max(Math.max(0, properties.getMaxAttempts() - 1))
            .filter(this::isRetryable)
            .onRetryExhaustedThrow((spec, signal) -> signal.failure());

    return webClient
        .put()
        .uri(PATH_TEMPLATE, tableUuid)
        .bodyValue(body)
        .retrieve()
        .toBodilessEntity()
        .timeout(Duration.ofMillis(properties.getPerAttemptTimeoutMs()))
        .doOnError(
            e ->
                meterRegistry
                    .counter(
                        MetricsConstant.OPTIMIZER_STATS_ATTEMPTS,
                        "outcome",
                        isRetryable(e) ? "retryable_failure" : "non_retryable_failure")
                    .increment())
        .doOnSuccess(
            ignored ->
                meterRegistry
                    .counter(MetricsConstant.OPTIMIZER_STATS_ATTEMPTS, "outcome", "success")
                    .increment())
        .retryWhen(retrySpec)
        .timeout(Duration.ofMillis(properties.getTotalTimeoutMs()))
        .doOnSuccess(
            ignored ->
                sample.stop(
                    meterRegistry.timer(
                        MetricsConstant.OPTIMIZER_STATS_DURATION, "outcome", "success")))
        .onErrorResume(
            e -> {
              String outcome = classifyOutcome(e);
              sample.stop(
                  meterRegistry.timer(
                      MetricsConstant.OPTIMIZER_STATS_DURATION, "outcome", outcome));
              meterRegistry
                  .counter(MetricsConstant.OPTIMIZER_STATS_FAILED_FINAL, "outcome", outcome)
                  .increment();
              log.warn(
                  "Optimizer stats push failed for table {} ({}): {}",
                  tableUuid,
                  outcome,
                  e.toString());
              return Mono.empty();
            })
        .then();
  }

  /** {@code true} iff the table's properties contain the literal opt-in value {@code "true"}. */
  private boolean isOptedIn(TableDto saved) {
    Map<String, String> props = saved.getTableProperties();
    return props != null && "true".equals(props.get(OPT_IN_PROPERTY));
  }

  /** Build the wire body. Missing summary keys default to 0L. */
  OptimizerStatsRequest buildRequest(TableDto saved, Map<String, String> summary) {
    OptimizerStatsRequest.Snapshot snapshot =
        OptimizerStatsRequest.Snapshot.builder()
            .tableVersion(saved.getTableVersion())
            .tableLocation(saved.getTableLocation())
            .tableSizeBytes(longOrZero(summary, SUMMARY_TOTAL_FILES_SIZE))
            .numCurrentFiles(longOrZero(summary, SUMMARY_TOTAL_DATA_FILES))
            .build();
    OptimizerStatsRequest.Delta delta =
        OptimizerStatsRequest.Delta.builder()
            .numFilesAdded(longOrZero(summary, SUMMARY_ADDED_DATA_FILES))
            .numFilesDeleted(longOrZero(summary, SUMMARY_DELETED_DATA_FILES))
            .addedSizeBytes(longOrZero(summary, SUMMARY_ADDED_FILES_SIZE))
            .deletedSizeBytes(longOrZero(summary, SUMMARY_REMOVED_FILES_SIZE))
            .build();
    return OptimizerStatsRequest.builder()
        .databaseName(saved.getDatabaseId())
        .tableName(saved.getTableId())
        .stats(OptimizerStatsRequest.Stats.builder().snapshot(snapshot).delta(delta).build())
        .tableProperties(saved.getTableProperties())
        .build();
  }

  /**
   * Retryable errors: per-attempt timeout, network-level failures, 5xx, 408, 429. Other 4xx are
   * client errors — retrying won't fix them.
   */
  boolean isRetryable(Throwable e) {
    if (e instanceof TimeoutException) {
      return true;
    }
    if (e instanceof WebClientRequestException) {
      return true;
    }
    if (e instanceof WebClientResponseException) {
      int code = ((WebClientResponseException) e).getStatusCode().value();
      return code == 408 || code == 429 || (code >= 500 && code < 600);
    }
    return false;
  }

  /**
   * Map a final-stage error to one of {@code success, timeout, network_error, server_error,
   * client_error, unknown_error} for the {@code outcome} tag on duration / failed_final metrics.
   */
  private String classifyOutcome(Throwable e) {
    if (e instanceof TimeoutException) {
      return "timeout";
    }
    if (e instanceof WebClientRequestException) {
      return "network_error";
    }
    if (e instanceof WebClientResponseException) {
      int code = ((WebClientResponseException) e).getStatusCode().value();
      if (code >= 500 && code < 600) {
        return "server_error";
      }
      if (code >= 400 && code < 500) {
        return "client_error";
      }
    }
    return "unknown_error";
  }

  private static long longOrZero(Map<String, String> m, String key) {
    String v = m.get(key);
    if (v == null) {
      return 0L;
    }
    try {
      return Long.parseLong(v);
    } catch (NumberFormatException nfe) {
      return 0L;
    }
  }
}
