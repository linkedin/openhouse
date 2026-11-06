package com.linkedin.openhouse.optimizer.analyzer;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import java.time.Instant;
import java.util.Optional;

/**
 * Strategy interface for a single operation type. Each implementation decides whether a given table
 * needs an operation recommendation upserted in the Optimizer Service.
 *
 * <p>Per-table retry backoff is in place: a chronically-failing table spaces out its retries
 * instead of producing a new PENDING row on every Analyzer pass. Once a (table, type) accrues
 * {@code failureStreakThreshold} consecutive non-success submissions (configurable per operation
 * type), the retry interval grows exponentially — capped at {@code failureBackoffMax} — and resets
 * to the flat interval on the next success, with an operator-visible WARN when it engages. Note
 * this spaces out retries for one table; it is not a dependency-level circuit breaker that stops
 * many tables from launching work against a shared failing dependency (a separate change).
 */
public interface OperationAnalyzer {

  /** The operation type this analyzer handles. */
  OperationTypeDto getOperationType();

  /**
   * Returns {@code true} if this operation is opted-in for the given table. Tables that return
   * {@code false} are skipped entirely — no upsert is issued.
   */
  boolean isEnabled(TableDto table);

  /**
   * Cheap pre-filter evaluated <i>before</i> any DB load on the commit-driven path: should a commit
   * to this table trigger analysis for this operation? Lets each analyzer "listen" only for the
   * changes it cares about, so we don't analyze every operation on every stat event.
   *
   * <p>Default {@code true} — react to every commit (table stats collection wants near-real-time
   * freshness). An operation that only cares about specific changes (e.g. a replication table
   * property change) overrides this to return {@code false} for irrelevant commits. This is only a
   * trigger pre-filter; the actual schedule-or-not decision still goes through {@link
   * #shouldSchedule} (opt-in, active-op dedup, cadence).
   *
   * @param table the committed table's current state
   */
  default boolean triggersOnCommit(TableDto table) {
    return true;
  }

  /**
   * Whether the given active op is a submitted job that has timed out — exceeded the stale-timeout
   * with no success. This "logical timeout" is derived, never persisted; the per-table retry
   * backoff counts it as a non-success alongside explicitly FAILED history.
   *
   * @param op an existing operation record for this (table, type)
   */
  boolean isActiveOpTimedOut(TableOperationDto op);

  /**
   * Earliest time to retry after an outcome observed at {@code from}: the success cadence on
   * success, otherwise the failure backoff for {@code attemptsSinceSuccess}. Persisted to {@code
   * available_at} on completion or expiry so retry timing no longer depends on the stale-timeout.
   */
  Instant nextAvailableAt(Instant from, boolean succeeded, int attemptsSinceSuccess);

  /**
   * Returns {@code true} if a new or refreshed operation record should be upserted.
   *
   * @param table the table entry
   * @param currentOp the existing active operation record, or empty if none exists
   * @param latestHistory the most recent history entry for this (table, type), or empty
   * @param consecutiveFailures number of consecutive non-success submissions ending at the latest
   *     one — explicitly FAILED history plus logical timeouts (submitted-without-success); {@code
   *     0} when the latest submission succeeded. Used by the per-table retry backoff to space out
   *     retries and signal a chronically-failing table.
   */
  boolean shouldSchedule(
      TableDto table,
      Optional<TableOperationDto> currentOp,
      Optional<TableOperationsHistoryDto> latestHistory,
      int consecutiveFailures);
}
