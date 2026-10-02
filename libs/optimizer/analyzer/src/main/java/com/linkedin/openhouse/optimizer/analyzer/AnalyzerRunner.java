package com.linkedin.openhouse.optimizer.analyzer;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.repository.TableOperationsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Core analysis loop. For one operation type per call, iterates databases and evaluates each table
 * in a database against the matching {@link OperationAnalyzer}.
 *
 * <p>Both sides of the join — current operations and latest history per (table, type) — are loaded
 * into maps once per database before the table loop. This is correct at small scale (≤~100k
 * tables); past that the per-db query shape and projection need further tuning.
 *
 * <p>The per-db working-set upper bound is not yet empirically validated.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AnalyzerRunner {

  private final List<OperationAnalyzer> analyzers;
  private final TableStatsRepository statsRepo;
  private final TableOperationsRepository operationsRepo;
  private final TableOperationsHistoryRepository historyRepo;

  /**
   * Run the analysis loop for {@code operationType} across all databases, with no filters.
   * Equivalent to {@link #analyze(OperationTypeDto, Optional, Optional, Optional)} with all-empty
   * filters.
   */
  public void analyze(OperationTypeDto operationType) {
    analyze(operationType, Optional.empty(), Optional.empty(), Optional.empty());
  }

  /**
   * Run the analysis loop for the given operation type, optionally scoped to a single database,
   * table name, or table UUID. Iterates databases one at a time so the working set is bounded by
   * tables-per-db, not tables-total.
   */
  public void analyze(
      OperationTypeDto operationType,
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid) {
    OperationAnalyzer analyzer =
        analyzers.stream()
            .filter(a -> a.getOperationType() == operationType)
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No analyzer registered for operation type " + operationType));
    List<String> dbs = databaseName.map(List::of).orElseGet(statsRepo::findDistinctDatabaseNames);
    log.info("Analyzing {} across {} database(s)", operationType, dbs.size());
    dbs.forEach(db -> analyzeDatabase(analyzer, db, tableName, tableUuid));
    log.info("Analysis complete for {}", operationType);
  }

  /**
   * Commit-driven entry point. Evaluates a single table — whose stats are already in memory from
   * the stats upsert, so {@code table_stats} is not re-read — against every registered analyzer,
   * under the same opt-in, active-op dedup, and cadence guards as the full scan. The table's
   * current operations and latest history are each loaded once and shared across analyzers.
   *
   * <p>Complements, not replaces, the full-scan {@link #analyze} the cron runs. Because the stats
   * arrive in memory, this cannot miss a brand-new table whose {@code table_stats} row is not yet
   * visible to a fetch-by-uuid.
   *
   * <p>Shares the per-table decision with the full-scan path via {@link #analyzeAndSchedule}; only
   * the load phase differs (in-memory here, DB query in {@link #analyzeDatabase}).
   */
  public void analyzeTable(TableDto table) {
    log.info(
        "Commit-driven analyze for table {}.{} (uuid={})",
        table.getDatabaseName(),
        table.getTableId(),
        table.getTableUuid());
    Map<OperationTypeDto, TableOperationDto> currentOps = loadCurrentOpsForTable(table);
    Map<OperationTypeDto, TableOperationsHistoryDto> latestHistory =
        loadLatestHistoryForTable(table);
    analyzers.forEach(
        analyzer ->
            analyzeAndSchedule(
                analyzer,
                table,
                Optional.ofNullable(currentOps.get(analyzer.getOperationType())),
                Optional.ofNullable(latestHistory.get(analyzer.getOperationType()))));
  }

  /** All active operations for a single table, keyed by operation type (most-recent per type). */
  private Map<OperationTypeDto, TableOperationDto> loadCurrentOpsForTable(TableDto table) {
    return operationsRepo
        .find(
            Optional.empty(),
            Optional.empty(),
            Optional.of(table.getTableUuid()),
            Optional.of(table.getDatabaseName()),
            Optional.of(table.getTableId()),
            Optional.empty(),
            Optional.empty(),
            Pageable.unpaged())
        .stream()
        .filter(e -> e.getTableUuid() != null)
        .map(TableOperationDto::fromRow)
        .collect(
            Collectors.toMap(
                TableOperationDto::getOperationType, op -> op, TableOperationDto::mostRecent));
  }

  /** Latest completed history entry per operation type for a single table. */
  private Map<OperationTypeDto, TableOperationsHistoryDto> loadLatestHistoryForTable(
      TableDto table) {
    return historyRepo.find(table.getTableUuid(), Pageable.unpaged()).stream()
        .filter(r -> r.getTableUuid() != null)
        .map(TableOperationsHistoryDto::fromRow)
        .collect(
            Collectors.toMap(
                TableOperationsHistoryDto::getOperationType,
                h -> h,
                TableOperationsHistoryDto::after));
  }

  @Transactional
  void analyzeDatabase(
      OperationAnalyzer analyzer,
      String databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid) {

    // Load the three join inputs unbounded for this database. Aligned page-by-page pagination on
    // these maps would leave keys in one map's page mismatched with the others' — a table whose
    // op/history happens to fall in a different page would be misread as "no current op / no
    // history" and trigger duplicate scheduling. Correctness requires the maps to be complete
    // relative to the tables being processed; the working set is bounded by tables-in-db, not by
    // any per-cycle cap.
    Map<String, TableOperationDto> currentOps =
        operationsRepo
            .find(
                Optional.of(analyzer.getOperationType().toDb()),
                Optional.empty(),
                tableUuid,
                Optional.of(databaseName),
                tableName,
                Optional.empty(),
                Optional.empty(),
                Pageable.unpaged())
            .stream()
            .filter(e -> e.getTableUuid() != null)
            .map(TableOperationDto::fromRow)
            .collect(
                Collectors.toMap(
                    TableOperationDto::getTableUuid, op -> op, TableOperationDto::mostRecent));

    Map<String, TableOperationsHistoryDto> latestHistory =
        historyRepo.findLatest(analyzer.getOperationType().toDb(), Pageable.unpaged()).stream()
            .filter(r -> r.getTableUuid() != null)
            .map(TableOperationsHistoryDto::fromRow)
            .collect(
                Collectors.toMap(
                    TableOperationsHistoryDto::getTableUuid,
                    h -> h,
                    TableOperationsHistoryDto::after));

    List<TableDto> tables =
        statsRepo.find(Optional.of(databaseName), tableName, tableUuid, Pageable.unpaged()).stream()
            .filter(row -> row.getTableUuid() != null)
            .map(TableDto::fromRow)
            .collect(Collectors.toList());

    /*
     * Process phase: for each table in this database, run the shared decision via
     * analyzeAndSchedule using the current op and latest-history maps loaded above. The full-scan
     * path differs from the commit-driven path only in this load phase; the decision is identical.
     */
    long created =
        tables.stream()
            .filter(
                table ->
                    analyzeAndSchedule(
                        analyzer,
                        table,
                        Optional.ofNullable(currentOps.get(table.getTableUuid())),
                        Optional.ofNullable(latestHistory.get(table.getTableUuid()))))
            .count();
    log.info(
        "Finished analyzing Database {}: created {} PENDING {} operation(s)",
        databaseName,
        created,
        analyzer.getOperationType());
  }

  /**
   * Process phase (shared by the commit-driven {@link #analyzeTable} and the full-scan {@link
   * #analyzeDatabase}): evaluate one {@code (analyzer, table)} against the table's already-loaded
   * current operation and latest history, and persist a PENDING operation when it is due. Keeping
   * this phase separate from the load phase lets the commit path reuse the in-memory stats while
   * sharing the identical opt-in / active-op / cadence decision. Returns whether a PENDING op was
   * created.
   */
  private boolean analyzeAndSchedule(
      OperationAnalyzer analyzer,
      TableDto table,
      Optional<TableOperationDto> currentOp,
      Optional<TableOperationsHistoryDto> latestHistory) {
    if (!analyzer.isEnabled(table) || !analyzer.shouldSchedule(table, currentOp, latestHistory)) {
      return false;
    }
    return createPending(analyzer, table);
  }

  /**
   * Persist one PENDING operation, isolating failures so a single bad table never aborts the rest
   * of the pass; the next pass retries it. Returns whether the save succeeded.
   */
  private boolean createPending(OperationAnalyzer analyzer, TableDto table) {
    try {
      operationsRepo.save(TableOperationDto.pending(table, analyzer.getOperationType()).toRow());
      log.debug(
          "Created PENDING {} operation for table {}.{}",
          analyzer.getOperationType(),
          table.getDatabaseName(),
          table.getTableId());
      return true;
    } catch (RuntimeException e) {
      // One bad table should not abort the rest of the database. Log and continue; the next
      // analyzer pass will retry for any table whose save failed here.
      log.error(
          "Failed to create PENDING {} operation for table {}.{}: {}",
          analyzer.getOperationType(),
          table.getDatabaseName(),
          table.getTableId(),
          e.toString(),
          e);
      return false;
    }
  }
}
