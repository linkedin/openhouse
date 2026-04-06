package com.linkedin.openhouse.optimizer.service;

import com.linkedin.openhouse.optimizer.api.model.CompleteOperationRequest;
import com.linkedin.openhouse.optimizer.api.model.OperationHistoryStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsDto;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.api.model.TableStatsDto;
import com.linkedin.openhouse.optimizer.api.model.TableStatsHistoryDto;
import com.linkedin.openhouse.optimizer.api.model.UpsertTableStatsRequest;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Service interface for optimizer data operations. */
public interface OptimizerDataService {

  // --- TableOperations ---

  /**
   * List operations matching the given filters. Every parameter is optional — pass {@code null} to
   * skip that filter. No filters returns all rows.
   */
  List<TableOperationsDto> listTableOperations(
      OperationType operationType,
      OperationStatus status,
      String databaseName,
      String tableName,
      String tableUuid);

  /**
   * Complete an operation by writing a history entry. Looks up the operation row by {@code id},
   * copies its table metadata into a new history row, and saves it. Returns the history DTO, or
   * empty if the operation does not exist.
   */
  Optional<TableOperationsHistoryDto> completeOperation(
      String id, CompleteOperationRequest request);

  /**
   * Return the operation row for {@code id} regardless of status, or empty if it does not exist.
   * Used to poll a specific operation (e.g. waiting for SUCCESS after a Spark job completes).
   */
  Optional<TableOperationsDto> getTableOperation(String id);

  // --- TableStats ---

  /**
   * Create or update the stats row for {@code tableUuid}. Fully idempotent: the same call
   * overwrites the previous snapshot with the latest commit values.
   */
  TableStatsDto upsertTableStats(String tableUuid, UpsertTableStatsRequest request);

  /** Return the stats row for {@code tableUuid}, or empty if none exists. */
  Optional<TableStatsDto> getTableStats(String tableUuid);

  /**
   * List stats rows matching the given filters. Every parameter is optional — pass {@code null} to
   * skip that filter. No filters returns all rows.
   */
  List<TableStatsDto> listTableStats(String databaseId, String tableName, String tableUuid);

  /**
   * Return per-commit stats history for {@code tableUuid}, newest first.
   *
   * @param tableUuid the stable table UUID
   * @param since if non-null, only return rows recorded at or after this instant
   * @param limit maximum number of rows to return
   */
  List<TableStatsHistoryDto> getStatsHistory(String tableUuid, Instant since, int limit);

  // --- TableOperationsHistory ---

  /** Append a completed-job result record. */
  TableOperationsHistoryDto appendHistory(TableOperationsHistoryDto dto);

  /**
   * Return the most recent history rows for a table UUID, newest first.
   *
   * @param tableUuid the stable table UUID
   * @param limit maximum number of rows to return
   */
  List<TableOperationsHistoryDto> getHistory(String tableUuid, int limit);

  /**
   * List history rows matching the given filters, ordered newest first. Every parameter is optional
   * — pass {@code null} to skip that filter. No filters returns all rows up to {@code limit}.
   */
  List<TableOperationsHistoryDto> listHistory(
      String databaseName,
      String tableName,
      String tableUuid,
      OperationType operationType,
      OperationHistoryStatus status,
      Instant since,
      Instant until,
      int limit);
}
