package com.linkedin.openhouse.optimizer.service;

import com.linkedin.openhouse.optimizer.model.HistoryStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import com.linkedin.openhouse.optimizer.model.TableStatsHistoryDto;
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
  List<TableOperationDto> listTableOperations(
      Optional<OperationTypeDto> operationType,
      Optional<OperationStatusDto> status,
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid);

  /**
   * Update an operation by writing a history entry. Looks up the operation row by {@code
   * operationId}, copies its table metadata into a new history row with the supplied terminal
   * {@code status}, and saves it. Returns the history record, or empty if the operation does not
   * exist.
   */
  Optional<TableOperationsHistoryDto> updateOperation(String operationId, HistoryStatusDto status);

  /**
   * Return the operation row for {@code id} regardless of status, or empty if it does not exist.
   * Used to poll a specific operation (e.g. waiting for SUCCESS after a Spark job completes).
   */
  Optional<TableOperationDto> getTableOperation(String id);

  // --- TableStatsDto ---

  /**
   * Create or update the stats row for {@code stats.getTableUuid()}. Fully idempotent: the same
   * call overwrites the previous snapshot with the latest commit values. The service stamps {@link
   * TableStatsDto#getUpdatedAt()} server-side and returns the resulting {@link TableStatsDto}.
   */
  TableStatsDto upsertTableStats(TableStatsDto stats);

  /** Return the stats row for {@code tableUuid}, or empty if none exists. */
  Optional<TableStatsDto> getTableStats(String tableUuid);

  /**
   * List stats rows matching the given filters. Every parameter is optional — pass {@link
   * Optional#empty()} to skip that filter. No filters returns all rows.
   */
  List<TableStatsDto> listTableStats(
      Optional<String> databaseName, Optional<String> tableName, Optional<String> tableUuid);

  /**
   * Return per-commit stats history for {@code tableUuid}, newest first.
   *
   * @param tableUuid the stable table UUID
   * @param since if present, only return rows recorded at or after this instant
   * @param limit maximum number of rows to return
   */
  List<TableStatsHistoryDto> getStatsHistory(String tableUuid, Optional<Instant> since, int limit);

  // --- TableOperationsHistoryDto ---

  /** Append a completed-job result record. */
  TableOperationsHistoryDto appendHistory(TableOperationsHistoryDto history);

  /**
   * Return the most recent history rows for a table UUID, newest first.
   *
   * @param tableUuid the stable table UUID
   * @param limit maximum number of rows to return
   */
  List<TableOperationsHistoryDto> getHistory(String tableUuid, int limit);
}
