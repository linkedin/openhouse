package com.linkedin.openhouse.tables.config;

import com.linkedin.openhouse.tables.model.TableDto;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Thin client that pushes a per-table stats snapshot to the optimizer service on every successful
 * Iceberg commit.
 *
 * <p>Activated by {@code cluster.optimizer.base-uri}. When absent (e.g. unit tests, dev clusters
 * without an optimizer), the bean is not created and {@link
 * com.linkedin.openhouse.tables.services.IcebergSnapshotsServiceImpl} skips the call.
 *
 * <p>The hook is best-effort: failures are logged but do not break the commit. The optimizer
 * analyzer re-reads stats on each cycle, so a transient post failure is recovered on the next
 * commit.
 */
@Component
@ConditionalOnProperty(name = "cluster.optimizer.base-uri")
@Slf4j
public class OptimizerTableStatsClient {

  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private final WebClient webClient;

  public OptimizerTableStatsClient(@Value("${cluster.optimizer.base-uri}") String baseUri) {
    this.webClient = WebClient.builder().baseUrl(baseUri).build();
  }

  /**
   * PUT a stats row for {@code table} to the optimizer service. Returns synchronously after the
   * write completes or fails; exceptions are caught and logged at WARN.
   */
  public void upsertTableStats(TableDto table) {
    String tableUuid = table.getTableUUID();
    if (tableUuid == null) {
      log.warn(
          "Skipping optimizer stats push for {}.{}: tableUUID is null",
          table.getDatabaseId(),
          table.getTableId());
      return;
    }
    Map<String, String> tableProperties =
        table.getTableProperties() == null ? Collections.emptyMap() : table.getTableProperties();
    UpsertStatsBody body =
        UpsertStatsBody.builder()
            .databaseName(table.getDatabaseId())
            .tableName(table.getTableId())
            .tableProperties(tableProperties)
            .build();
    try {
      webClient
          .put()
          .uri("/v1/optimizer/stats/{tableUuid}", tableUuid)
          .bodyValue(body)
          .retrieve()
          .toBodilessEntity()
          .block(TIMEOUT);
    } catch (Exception e) {
      log.warn(
          "Optimizer stats push failed for tableUuid={} ({}.{}): {}",
          tableUuid,
          table.getDatabaseId(),
          table.getTableId(),
          e.getMessage());
    }
  }

  /**
   * Wire body matching {@link
   * com.linkedin.openhouse.optimizer.api.spec.UpsertTableStatsRequestDto}. Kept as a local
   * representation so this module does not need to depend on the optimizer api module.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  static class UpsertStatsBody {
    private String databaseName;
    private String tableName;
    private Map<String, String> tableProperties;
  }
}
