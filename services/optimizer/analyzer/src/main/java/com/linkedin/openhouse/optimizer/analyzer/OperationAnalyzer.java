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
   * Returns {@code true} if a new or refreshed operation record should be upserted.
   *
   * @param table the table entry
   * @param currentOp the existing active operation record, or empty if none exists
   * @param latestHistory the most recent history entry for this (table, type), or empty
   */
  boolean shouldSchedule(
      TableDto table,
      Optional<TableOperationDto> currentOp,
      Optional<TableOperationsHistoryDto> latestHistory);
}
