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
 * tables); past that the per-db query shape and projection need further tuning. Scale-up work is
 * tracked in <a href="https://linkedin.atlassian.net/browse/BDP-102182">BDP-102182</a>.
 *
 * <p>The per-db working-set upper bound is not yet empirically validated. Scale-test tracked in <a
 * href="https://linkedin.atlassian.net/browse/BDP-102738">BDP-102738</a>.
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
    // any per-cycle cap. Memory characterization is tracked in BDP-102738.
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
     * For each table in this database, decide whether to create a new PENDING operation.
     *
     * 1. Skip tables not opted in to this operation type.
     * 2. Look up the table's current active operation (if any) and its most recent completed
     *    history entry from the maps loaded above.
     * 3. Delegate the schedule-or-not decision to the analyzer's shouldSchedule — strategy
     *    encapsulates cadence, retry policy, and any future per-operation signals.
     * 4. On true, persist a new PENDING operation. The scheduler picks it up on its next pass.
     */
    int created = 0;
    int failed = 0;
    for (TableDto table : tables) {
      if (!analyzer.isEnabled(table)) {
        continue;
      }
      Optional<TableOperationDto> currentOp =
          Optional.ofNullable(currentOps.get(table.getTableUuid()));
      Optional<TableOperationsHistoryDto> entry =
          Optional.ofNullable(latestHistory.get(table.getTableUuid()));
      if (!analyzer.shouldSchedule(table, currentOp, entry)) {
        continue;
      }
      try {
        TableOperationDto op = TableOperationDto.pending(table, analyzer.getOperationType());
        operationsRepo.save(op.toRow());
        log.info(
            "Created PENDING {} operation for table {}.{}",
            analyzer.getOperationType(),
            table.getDatabaseName(),
            table.getTableId());
        created++;
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
        failed++;
      }
    }
    log.info(
        "Finished analyzing Database {}: created {} PENDING {} operation(s) ({} failed)",
        databaseName,
        created,
        analyzer.getOperationType(),
        failed);
  }
}
