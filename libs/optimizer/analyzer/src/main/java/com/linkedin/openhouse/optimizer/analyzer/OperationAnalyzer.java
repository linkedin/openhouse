package com.linkedin.openhouse.optimizer.analyzer;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
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
   * Whether this operation may run on a replica table. A replica mirrors a primary in another
   * cluster via replication, so any operation that would change the replica's committed Iceberg
   * metadata or snapshots (e.g. snapshot expiration, retention, compaction, data-layout) must not
   * run on it — it would diverge the replica from its primary. Operations that don't touch
   * committed metadata are safe: read-only ones like stats collection, and orphan-file deletion
   * (which only removes storage files not referenced by any snapshot, and must run on the replica
   * since the primary's OFD cleans only the primary's storage).
   *
   * <p>Defaults to {@code false} so the metadata/snapshot-mutating maintenance case is primary-only
   * by default; an operation that is safe on a replica opts in by overriding to {@code true}.
   * Enforced centrally by {@code AnalyzerRunner} on both the commit-driven and full-scan paths, so
   * a replica is skipped before {@link #isEnabled}/{@link #shouldSchedule} are consulted.
   */
  default boolean runsOnReplica() {
    return false;
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
