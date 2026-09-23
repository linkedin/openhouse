package com.linkedin.openhouse.optimizer.analyzer;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Decides when to schedule an Orphan-Files-Deletion (OFD) run for a table.
 *
 * <p>OFD removes data files in the table's storage directory that are no longer referenced by any
 * Iceberg snapshot — left-over output from failed writes, expired snapshots, or interrupted
 * compactions. Running it too often wastes compute; running it too rarely lets orphan files
 * accumulate and bloats storage cost.
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
 *             scheduling again.
 *         <li>If the most recent history entry is {@code FAILED}, wait {@code
 *             ofd.failure-retry-hours} (default 1h) before retrying.
 *       </ul>
 *   <li><b>Data-driven activity gate.</b> Once the cadence window has elapsed, the table is
 *       scheduled only if it has actually been <i>written</i> since its last completed run — i.e.
 *       {@code table_stats.updatedAt} (stamped on every commit-stats publish) is newer than the
 *       last run's {@code completedAt}. New commits are the source of new orphans, so a table with
 *       no commits since its last sweep has nothing to clean and is skipped. This is where the
 *       compute savings come from at scale: idle tables no longer pay for an OFD every cadence
 *       window. As a safety net, an idle table is still swept once it has been idle longer than
 *       {@code ofd.max-idle-hours} (default 168h / 7d), which catches orphans left by failed writes
 *       (those produce no successful commit and therefore do not advance {@code updatedAt}).
 * </ol>
 *
 * <p>The retry and max-idle intervals are configurable via {@code application.properties}. The
 * opt-in property is per-table and managed through the standard table-properties API. Note that a
 * table only appears in the optimizer once it publishes commit stats, so {@code updatedAt} is a
 * reliable per-commit signal for opted-in tables.
 */
@Component
public class CadenceBasedOrphanFilesDeletionAnalyzer implements OperationAnalyzer {

  static final String OFD_ENABLED_PROPERTY = "maintenance.optimizer.ofd.enabled";

  private final CadencePolicy cadencePolicy;
  private final Duration maxIdleInterval;

  public CadenceBasedOrphanFilesDeletionAnalyzer(
      @Value("${ofd.success-retry-hours:16}") long successRetryHours,
      @Value("${ofd.failure-retry-hours:1}") long failureRetryHours,
      @Value("${ofd.max-idle-hours:168}") long maxIdleHours) {
    this.cadencePolicy =
        new CadencePolicy(Duration.ofHours(successRetryHours), Duration.ofHours(failureRetryHours));
    this.maxIdleInterval = Duration.ofHours(maxIdleHours);
  }

  /** Package-private for tests that supply a pre-built {@link CadencePolicy}. */
  CadenceBasedOrphanFilesDeletionAnalyzer(CadencePolicy cadencePolicy, Duration maxIdleInterval) {
    this.cadencePolicy = cadencePolicy;
    this.maxIdleInterval = maxIdleInterval;
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
    // Timing/retry gate first: an active op blocks, and the success/failure cadence must have
    // elapsed. A first-ever run (no history) passes this gate.
    if (!cadencePolicy.shouldSchedule(currentOp, latestHistory)) {
      return false;
    }
    // Data-driven gate: only schedule if the table has been written since its last completed run
    // (new commits => potential new orphans), or if it has been idle past the safety-net window.
    return committedSinceLastRun(table, latestHistory) || idleGraceElapsed(latestHistory);
  }

  /**
   * True if the table has committed since its last completed run — {@code table_stats.updatedAt} is
   * newer than the last run's {@code completedAt}. A table with no prior history is treated as
   * needing a first run.
   */
  private static boolean committedSinceLastRun(
      TableDto table, Optional<TableOperationsHistoryDto> latestHistory) {
    if (!latestHistory.isPresent()) {
      return true;
    }
    Instant lastRun = latestHistory.get().getCompletedAt();
    Instant lastCommit = table.getUpdatedAt();
    return lastCommit != null && lastRun != null && lastCommit.isAfter(lastRun);
  }

  /** True once a table has been idle (no commits) longer than {@link #maxIdleInterval}. */
  private boolean idleGraceElapsed(Optional<TableOperationsHistoryDto> latestHistory) {
    return latestHistory
        .map(
            h -> Duration.between(h.getCompletedAt(), Instant.now()).compareTo(maxIdleInterval) > 0)
        .orElse(true);
  }
}
