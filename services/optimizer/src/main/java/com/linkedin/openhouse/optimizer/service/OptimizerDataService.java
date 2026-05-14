package com.linkedin.openhouse.optimizer.service;

import com.linkedin.openhouse.optimizer.model.HistoryStatus;
import com.linkedin.openhouse.optimizer.model.OperationStatus;
import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.Table;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistory;
import com.linkedin.openhouse.optimizer.model.TableStatsHistory;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Service interface for optimizer data operations.
 *
 * <p>The service is the boundary between the wire-API surface and the database. Inputs and outputs
 * are <em>internal-model</em> types only — callers (controllers, future CLI, in-process consumers)
 * convert at their own edge. No api/-package types appear here.
 */
public interface OptimizerDataService {

  // --- TableOperations ---

  /**
   * List operations matching the given filters. Every parameter is optional — pass {@link
   * Optional#empty()} to skip that filter. No filters returns all rows.
   */
  List<TableOperation> listTableOperations(
      Optional<OperationType> operationType,
      Optional<OperationStatus> status,
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid);

  /**
   * Complete an operation by writing a history entry. Looks up the operation row by {@code
   * operationId}, copies its table metadata into a new history row with the supplied terminal
   * {@code status}, and saves it. Returns the history record, or empty if the operation does not
   * exist.
   */
  Optional<TableOperationsHistory> completeOperation(String operationId, HistoryStatus status);

  /**
   * Return the operation row for {@code id} regardless of status, or empty if it does not exist.
   * Used to poll a specific operation (e.g. waiting for SUCCESS after a Spark job completes).
   */
  Optional<TableOperation> getTableOperation(String id);

  // --- TableStats ---

  /**
   * Create or update the stats row for {@code table.getTableUuid()}. Fully idempotent: the same
   * call overwrites the previous snapshot with the latest commit values. The service stamps {@link
   * Table#getUpdatedAt()} server-side and returns the resulting {@link Table}.
   */
  Table upsertTableStats(Table table);

  /** Return the stats row for {@code tableUuid}, or empty if none exists. */
  Optional<Table> getTableStats(String tableUuid);

  /**
   * List stats rows matching the given filters. Every parameter is optional — pass {@link
   * Optional#empty()} to skip that filter. No filters returns all rows.
   */
  List<Table> listTableStats(
      Optional<String> databaseName, Optional<String> tableName, Optional<String> tableUuid);

  /**
   * Return per-commit stats history for {@code tableUuid}, newest first.
   *
   * @param tableUuid the stable table UUID
   * @param since if present, only return rows recorded at or after this instant
   * @param limit maximum number of rows to return
   */
  List<TableStatsHistory> getStatsHistory(String tableUuid, Optional<Instant> since, int limit);

  // --- TableOperationsHistory ---

  /** Append a completed-job result record. */
  TableOperationsHistory appendHistory(TableOperationsHistory history);

  /**
   * Return the most recent history rows for a table UUID, newest first.
   *
   * @param tableUuid the stable table UUID
   * @param limit maximum number of rows to return
   */
  List<TableOperationsHistory> getHistory(String tableUuid, int limit);
}
