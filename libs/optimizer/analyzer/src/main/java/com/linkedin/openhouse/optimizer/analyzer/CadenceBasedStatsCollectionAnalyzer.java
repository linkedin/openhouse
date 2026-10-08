package com.linkedin.openhouse.optimizer.analyzer;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import java.time.Duration;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Decides when to schedule a Table-Stats-Collection run for a table.
 *
 * <p>Stats collection refreshes the per-table snapshot/size metrics the optimizer relies on. It is
 * cheap but not free, so it runs on a per-table cadence rather than on every commit.
 *
 * <h2>When stats collection fires for a table</h2>
 *
 * All of the following must be true:
 *
 * <ol>
 *   <li><b>Opt-in.</b> The table sets {@code maintenance.optimizer.stats.enabled=true} in its table
 *       properties. Without this flag, the analyzer ignores the table entirely.
 *   <li><b>No <i>live</i> operation already in flight.</b> If the table has a non-CANCELED
 *       operation row — PENDING or in-progress (SCHEDULING/SCHEDULED) — the scheduler owns it and
 *       the analyzer stays out. A CANCELED row does not block. <b>Exception (deadline):</b> a
 *       SCHEDULING/SCHEDULED row whose job has not reached a terminal state within {@code
 *       stats.stale-timeout-hours} (default 3h) is treated as dead (hung/missed callback) and no
 *       longer blocks, so the table is rescheduled instead of wedged. Stats biases toward an extra
 *       job (short deadline) since collection is idempotent.
 *   <li><b>Cadence elapsed since the last completed run.</b>
 *       <ul>
 *         <li>If the table has <i>no</i> prior history, schedule immediately.
 *         <li>If the most recent run {@code SUCCESS}, wait {@code stats.success-retry-hours}
 *             (default 24h) after its {@code completedAt} before collecting again.
 *         <li>If the most recent run {@code FAILED}, retry after {@code stats.failure-retry-hours}
 *             (default 1h) — shorter than the 24h success cadence so a failed collection recovers
 *             sooner without waiting out the full success interval.
 *       </ul>
 * </ol>
 *
 * <p>All intervals are configurable via {@code application.properties}. The opt-in property is
 * per-table and managed through the standard table-properties API.
 *
 * <p><b>Global toggle.</b> Registered as a bean unless {@code analyzer.stats.enabled=false} (on by
 * default). This deployment-wide flag is distinct from the per-table opt-in property {@code
 * maintenance.optimizer.stats.enabled}.
 */
@Component
@Slf4j
@ConditionalOnProperty(name = "analyzer.stats.enabled", havingValue = "true", matchIfMissing = true)
public class CadenceBasedStatsCollectionAnalyzer implements OperationAnalyzer {

  static final String STATS_ENABLED_PROPERTY = "maintenance.optimizer.stats.enabled";

  private final CadencePolicy cadencePolicy;

  @Autowired
  public CadenceBasedStatsCollectionAnalyzer(
      @Value("${stats.success-retry-hours:24}") long successRetryHours,
      @Value("${stats.failure-retry-hours:1}") long failureRetryHours,
      @Value("${stats.stale-timeout-hours:3}") long staleTimeoutHours,
      @Value("${stats.failure-streak-threshold:3}") int failureStreakThreshold,
      @Value("${stats.failure-backoff-max-hours:24}") long failureBackoffMaxHours) {
    // Stats collection is idempotent and does not mutate the table, so it biases toward an "extra
    // job": a short active-op deadline reschedules a stuck SCHEDULED job sooner — see
    // CadencePolicy. After failureStreakThreshold consecutive failures the circuit breaker backs
    // off exponentially so a chronically-failing table stops burning jobs.
    this.cadencePolicy =
        new CadencePolicy(
            Duration.ofHours(successRetryHours),
            Duration.ofHours(failureRetryHours),
            Duration.ofHours(staleTimeoutHours),
            failureStreakThreshold,
            Duration.ofHours(failureBackoffMaxHours));
  }

  /** Package-private for tests that supply a pre-built {@link CadencePolicy}. */
  CadenceBasedStatsCollectionAnalyzer(CadencePolicy cadencePolicy) {
    this.cadencePolicy = cadencePolicy;
  }

  @Override
  public OperationTypeDto getOperationType() {
    return OperationTypeDto.TABLE_STATS_COLLECTION;
  }

  @Override
  public boolean isEnabled(TableDto table) {
    return "true".equals(table.getTableProperties().get(STATS_ENABLED_PROPERTY));
  }

  @Override
  public boolean shouldSchedule(
      TableDto table,
      Optional<TableOperationDto> currentOp,
      Optional<TableOperationsHistoryDto> latestHistory,
      int consecutiveFailures) {
    if (cadencePolicy.isBreakerTripped(consecutiveFailures)) {
      // Operator-visible signal for a chronically-failing table (circuit breaker tripped). TODO:
      // emit a metric here once a MeterRegistry is on the optimizer classpath; the breaker then
      // backs off exponentially rather than rescheduling every cadence.
      log.warn(
          "Stats-collection circuit breaker tripped for {}.{} (uuid={}): {} consecutive failures; backing off",
          table.getDatabaseName(),
          table.getTableId(),
          table.getTableUuid(),
          consecutiveFailures);
    }
    return cadencePolicy.shouldSchedule(currentOp, latestHistory, consecutiveFailures);
  }
}
