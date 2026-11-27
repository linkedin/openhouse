package com.linkedin.openhouse.optimizer.analyzer;

import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import java.util.Optional;

/**
 * Receives every table one analyzer evaluates during a single full or filtered scan, then the end
 * of that scan. {@link AnalyzerRunner} creates one observer per analyzer per scan through {@link
 * OperationAnalyzer#startScan()}, so an observer can aggregate across all databases in the scan.
 * The commit-driven path evaluates one table at a time and does not notify observers.
 */
public interface ScanObserver {

  /**
   * Called once for each table the scan evaluated for this analyzer.
   *
   * @param table the evaluated table
   * @param latestHistory the table's latest history entry for this analyzer's operation type
   * @param operationCreated whether the scan created a PENDING operation for the table
   */
  default void tableAnalyzed(
      TableDto table,
      Optional<TableOperationsHistoryDto> latestHistory,
      boolean operationCreated) {}

  /** Called after the scan has evaluated every table in every selected database. */
  default void scanCompleted() {}
}
