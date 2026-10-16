package com.linkedin.openhouse.optimizer.analyzer;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import java.time.Duration;
import java.util.Optional;
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
 *   <li><b>No active operation already in flight.</b> If the table has a non-CANCELED operation row
 *       — either PENDING or in-progress (SCHEDULING/SCHEDULED) — the scheduler already owns it and
 *       the analyzer stays out. A CANCELED row does not block.
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
 * <p>Both intervals are configurable via {@code application.properties}. The opt-in property is
 * per-table and managed through the standard table-properties API.
 *
 * <p><b>Global toggle.</b> Registered as a bean unless {@code analyzer.stats.enabled=false} (on by
 * default). This deployment-wide flag is distinct from the per-table opt-in property {@code
 * maintenance.optimizer.stats.enabled}.
 */
@Component
@ConditionalOnProperty(name = "analyzer.stats.enabled", havingValue = "true", matchIfMissing = true)
public class CadenceBasedStatsCollectionAnalyzer implements OperationAnalyzer {

  static final String STATS_ENABLED_PROPERTY = "maintenance.optimizer.stats.enabled";

  private final CadencePolicy cadencePolicy;

  @Autowired
  public CadenceBasedStatsCollectionAnalyzer(
      @Value("${stats.success-retry-hours:24}") long successRetryHours,
      @Value("${stats.failure-retry-hours:1}") long failureRetryHours) {
    this.cadencePolicy =
        new CadencePolicy(Duration.ofHours(successRetryHours), Duration.ofHours(failureRetryHours));
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
      Optional<TableOperationsHistoryDto> latestHistory) {
    return cadencePolicy.shouldSchedule(currentOp, latestHistory);
  }
}
