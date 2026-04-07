package com.linkedin.openhouse.analyzer;

import com.linkedin.openhouse.analyzer.model.Table;
import com.linkedin.openhouse.analyzer.model.TableOperation;
import com.linkedin.openhouse.optimizer.entity.TableOperationHistoryRow;
import java.util.List;
import java.util.Optional;

/**
 * Strategy interface for a single operation type. Each implementation decides whether a given table
 * needs an operation recommendation upserted in the Optimizer Service.
 */
public interface OperationAnalyzer {

  /** The operation type this analyzer handles (e.g., {@code "ORPHAN_FILES_DELETION"}). */
  String getOperationType();

  /**
   * Returns {@code true} if this operation is opted-in for the given table. Tables that return
   * {@code false} are skipped entirely — no upsert is issued.
   */
  boolean isEnabled(Table table);

  /**
   * Returns {@code true} if a new or refreshed operation record should be upserted.
   *
   * @param table the table entry
   * @param currentOp the existing active operation record, or empty if none exists
   * @param latestHistory the most recent history entry for this (table, type), or empty
   */
  boolean shouldSchedule(
      Table table,
      Optional<TableOperation> currentOp,
      Optional<TableOperationHistoryRow> latestHistory);

  /**
   * Maximum number of consecutive FAILED history entries before the circuit breaker trips and
   * scheduling is suppressed for this (table, operation_type). Override per operation type. Returns
   * 0 to disable the circuit breaker.
   */
  default int getCircuitBreakerThreshold() {
    return 5;
  }

  /**
   * Returns {@code true} if the circuit breaker has tripped for this table. The default
   * implementation checks whether the last N history entries are all FAILED. Individual analyzers
   * can override this to implement different strategies (e.g., time-based backoff).
   *
   * <p>// TODO: Add circuit breaker reset with exponential backoff so tables can recover
   * automatically after a cooldown period instead of staying tripped permanently.
   *
   * <p>// TODO: Add a communication path to surface tripped circuit breakers to users (e.g.,
   * metrics, alerts, or a dashboard query).
   *
   * @param tableUuid the table whose history to check
   * @param history recent history entries for this (table, type), newest first
   */
  default boolean isCircuitBroken(String tableUuid, List<TableOperationHistoryRow> history) {
    int threshold = getCircuitBreakerThreshold();
    if (threshold <= 0 || history.size() < threshold) {
      return false;
    }
    return history.stream().limit(threshold).allMatch(r -> "FAILED".equals(r.getStatus()));
  }
}
