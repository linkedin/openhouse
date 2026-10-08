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
 * <p>TODO(circuit-breaker): a chronically-failing table currently produces a new PENDING row on
 * every Analyzer pass. Add a circuit breaker that suppresses scheduling for a (table, type) after N
 * consecutive FAILED history entries. Requirements: configurable threshold per operation type,
 * automatic reset via exponential backoff so tables can recover, and an operator-visible signal
 * (metric or query) so tripped breakers are diagnosable.
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
   * Returns {@code true} if a new or refreshed operation record should be upserted.
   *
   * @param table the table entry
   * @param currentOp the existing active operation record, or empty if none exists
   * @param latestHistory the most recent history entry for this (table, type), or empty
   * @param consecutiveFailures number of consecutive FAILED history entries ending at {@code
   *     latestHistory} ({@code 0} when the latest run did not fail), used by the circuit breaker to
   *     back off and signal a chronically-failing table
   */
  boolean shouldSchedule(
      TableDto table,
      Optional<TableOperationDto> currentOp,
      Optional<TableOperationsHistoryDto> latestHistory,
      int consecutiveFailures);
}
