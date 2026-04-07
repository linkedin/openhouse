package com.linkedin.openhouse.analyzer;

import com.linkedin.openhouse.analyzer.model.TableOperationRecord;
import com.linkedin.openhouse.analyzer.model.TableSummary;
import com.linkedin.openhouse.optimizer.entity.TableOperationHistoryRow;
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
  boolean isEnabled(TableSummary table);

  /**
   * Returns {@code true} if a new or refreshed operation record should be upserted.
   *
   * @param table the table entry
   * @param currentOp the existing active operation record, or empty if none exists
   * @param latestHistory the most recent history entry for this (table, type), or empty
   */
  boolean shouldSchedule(
      TableSummary table,
      Optional<TableOperationRecord> currentOp,
      Optional<TableOperationHistoryRow> latestHistory);

  /**
   * Maximum number of consecutive FAILED history entries before the circuit breaker trips and
   * scheduling is suppressed for this (table, operation_type). Override per operation type. Returns
   * 0 to disable the circuit breaker.
   */
  default int getCircuitBreakerThreshold() {
    return 5;
  }
}
