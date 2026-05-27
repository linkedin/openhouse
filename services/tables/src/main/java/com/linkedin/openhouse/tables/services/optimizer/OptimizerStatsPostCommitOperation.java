package com.linkedin.openhouse.tables.services.optimizer;

import com.linkedin.openhouse.tables.model.CurrentSnapshotInfo;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.services.postcommit.PostCommitOperation;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
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
 * {@link PostCommitOperation} that PUTs a snapshot-stats record to the optimizer's per-table stats
 * endpoint. {@link #prepare(TableDto)} returns a {@link Mono} that completes on HTTP 2xx and
 * signals an error otherwise; the dispatcher owns timeout, subscription, error swallowing, and
 * metric emission.
 *
 * <p>Skipped (operation returns {@link Optional#empty()}) when the table is not opted in via the
 * {@link #OPT_IN_PROPERTY} table property, or when no current snapshot is present (e.g. {@code
 * CREATE TABLE} with no rows yet).
 *
 * <p>Internal retry: bounded by {@link OptimizerStatsProperties#getMaxAttempts()}, fires only on
 * retryable errors (network, {@link TimeoutException}, HTTP 408 / 429 / 5xx). The dispatcher's
 * outer per-op timeout is the hard ceiling on the whole chain.
 *
 * <p>Bean is only wired when {@code optimizer.stats.enabled=true}. The path constant is
 * intentionally duplicated from {@code TableStatsController.TABLE_PATH_TEMPLATE}; keep in sync.
 * Tables service does not take a compile-time dependency on the optimizer service jar.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "optimizer.stats", name = "enabled", havingValue = "true")
public class OptimizerStatsPostCommitOperation implements PostCommitOperation {

  /** Metric tag value for {@code op}. */
  static final String OP_NAME = "optimizer_stats";

  /**
   * Per-call URL path. Intentionally duplicated from {@code
   * TableStatsController.TABLE_PATH_TEMPLATE}; keep in sync.
   */
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
  private final OptimizerStatsProperties properties;

  public OptimizerStatsPostCommitOperation(
      @Qualifier("optimizerStatsWebClient") WebClient webClient,
      OptimizerStatsProperties properties) {
    this.webClient = webClient;
    this.properties = properties;
  }

  @Override
  public String name() {
    return OP_NAME;
  }

  @Override
  public Optional<Mono<Void>> prepare(TableDto savedDto) {
    if (!isOptedIn(savedDto)) {
      return Optional.empty();
    }
    Optional<CurrentSnapshotInfo> snapshot = savedDto.getCurrentSnapshot();
    if (!snapshot.isPresent()) {
      return Optional.empty();
    }

    OptimizerStatsRequest body = buildRequest(savedDto, snapshot.get());
    String tableUuid = savedDto.getTableUUID();

    RetrySpec retrySpec =
        Retry.max(Math.max(0, properties.getMaxAttempts() - 1)).filter(this::isRetryable);

    Mono<Void> chain =
        webClient
            .put()
            .uri(PATH_TEMPLATE, tableUuid)
            .bodyValue(body)
            .retrieve()
            .toBodilessEntity()
            .timeout(Duration.ofMillis(properties.getPerAttemptTimeoutMs()))
            .retryWhen(retrySpec.onRetryExhaustedThrow((spec, signal) -> signal.failure()))
            .then();
    return Optional.of(chain);
  }

  /** {@code true} iff the table's properties contain the literal opt-in value {@code "true"}. */
  private boolean isOptedIn(TableDto saved) {
    Map<String, String> props = saved.getTableProperties();
    return props != null && "true".equals(props.get(OPT_IN_PROPERTY));
  }

  /** Build the wire body. Missing summary keys default to 0L. */
  OptimizerStatsRequest buildRequest(TableDto saved, CurrentSnapshotInfo snapshot) {
    Map<String, String> summary = snapshot.getSummary();
    OptimizerStatsRequest.Snapshot snapshotPayload =
        OptimizerStatsRequest.Snapshot.builder()
            .snapshotId(snapshot.getSnapshotId())
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
        .stats(OptimizerStatsRequest.Stats.builder().snapshot(snapshotPayload).delta(delta).build())
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

  private static long longOrZero(Map<String, String> m, String key) {
    if (m == null) {
      return 0L;
    }
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
