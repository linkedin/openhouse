package com.linkedin.openhouse.analyzer;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import java.time.Duration;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Decides when to schedule an Orphan-Files-Deletion (OFD) run for a table.
 *
 * <p>OFD removes data files in the table's storage directory that are no longer referenced by any
 * Iceberg snapshot — left-over output from failed writes, expired snapshots, or interrupted
 * compactions. Running it too often wastes compute; running it too rarely lets orphan files
 * accumulate and bloats storage cost. This analyzer balances the two on a per-table cadence.
 *
 * <h2>When OFD fires for a table</h2>
 *
 * All of the following must be true:
 *
 * <ol>
 *   <li><b>Opt-in.</b> The table sets {@code maintenance.optimizer.ofd.enabled=true} in its table
 *       properties. Without this flag, the analyzer ignores the table entirely.
 *   <li><b>No active operation already in flight.</b> If the table has a non-CANCELED operation row
 *       (PENDING, SCHEDULING, or SCHEDULED), the scheduler already owns it and the analyzer stays
 *       out. A CANCELED row does not block — it is treated as if no operation exists.
 *   <li><b>Cadence elapsed since the last completed run.</b>
 *       <ul>
 *         <li>If the table has <i>no</i> prior history, schedule immediately.
 *         <li>If the most recent history entry is {@code SUCCESS}, wait {@code
 *             ofd.success-retry-hours} (default 16h) after its {@code completedAt} before
 *             scheduling again. Set below 24h so that even when a run lands at an unlucky time of
 *             day, at least one re-evaluation is guaranteed within any rolling 24-hour window.
 *         <li>If the most recent history entry is {@code FAILED}, wait {@code
 *             ofd.failure-retry-hours} (default 1h) before retrying — shorter than the success
 *             interval so transient failures recover quickly.
 *       </ul>
 * </ol>
 *
 * <p>The two retry intervals are configurable via {@code application.properties} and can be tuned
 * per environment. The opt-in property is per-table and managed through the standard table-
 * properties API.
 */
@Component
public class CadenceBasedOrphanFilesDeletionAnalyzer implements OperationAnalyzer {

  static final String OFD_ENABLED_PROPERTY = "maintenance.optimizer.ofd.enabled";

  private final CadencePolicy cadencePolicy;

  public CadenceBasedOrphanFilesDeletionAnalyzer(
      @Value("${ofd.success-retry-hours:16}") long successRetryHours,
      @Value("${ofd.failure-retry-hours:1}") long failureRetryHours) {
    this.cadencePolicy =
        new CadencePolicy(Duration.ofHours(successRetryHours), Duration.ofHours(failureRetryHours));
  }

  /** Package-private for tests that supply a pre-built {@link CadencePolicy}. */
  CadenceBasedOrphanFilesDeletionAnalyzer(CadencePolicy cadencePolicy) {
    this.cadencePolicy = cadencePolicy;
  }

  @Override
  public OperationTypeDto getOperationType() {
    return OperationTypeDto.ORPHAN_FILES_DELETION;
  }

  @Override
  public boolean isEnabled(TableDto table) {
    return "true".equals(table.getTableProperties().get(OFD_ENABLED_PROPERTY));
  }

  @Override
  public boolean shouldSchedule(
      TableDto table,
      Optional<TableOperationDto> currentOp,
      Optional<TableOperationsHistoryDto> latestHistory) {
    return cadencePolicy.shouldSchedule(currentOp, latestHistory);
  }
}
