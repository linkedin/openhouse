package com.linkedin.openhouse.analyzer;

import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.Table;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistory;
import com.linkedin.openhouse.optimizer.repository.TableOperationsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Core analysis loop. For one operation type per call, iterates databases and evaluates each table
 * in a database against the matching {@link OperationAnalyzer}.
 *
 * <p>Both sides of the join — current operations and latest history per (table, type) — are loaded
 * into maps once per database before the table loop. This is correct at small scale (≤~100k
 * tables); past that the per-db query shape and projection need further tuning. Scale-up work is
 * tracked in <a href="https://linkedin.atlassian.net/browse/BDP-102182">BDP-102182</a>.
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
   * Equivalent to {@link #analyze(OperationType, Optional, Optional, Optional)} with all-empty
   * filters.
   */
  public void analyze(OperationType operationType) {
    analyze(operationType, Optional.empty(), Optional.empty(), Optional.empty());
  }

  /**
   * Run the analysis loop for the given operation type, optionally scoped to a single database,
   * table name, or table UUID. Iterates databases one at a time so the working set is bounded by
   * tables-per-db, not tables-total.
   */
  public void analyze(
      OperationType operationType,
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid) {
    Optional<OperationAnalyzer> analyzerOpt =
        analyzers.stream().filter(a -> a.getOperationType() == operationType).findFirst();
    if (analyzerOpt.isEmpty()) {
      log.warn("No analyzer registered for operation type {}; skipping", operationType);
      return;
    }
    OperationAnalyzer analyzer = analyzerOpt.get();
    List<String> dbs = databaseName.map(List::of).orElseGet(statsRepo::findDistinctDatabaseNames);
    log.info("Analyzing {} across {} database(s)", operationType, dbs.size());
    dbs.forEach(db -> analyzeDatabase(analyzer, db, tableName, tableUuid));
    log.info("Analysis complete for {}", operationType);
  }

  private void analyzeDatabase(
      OperationAnalyzer analyzer,
      String databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid) {

    com.linkedin.openhouse.optimizer.db.OperationType dbOperationType =
        analyzer.getOperationType().toDb();

    // Pre-load the small sides of the joins — bounded by tables in this database.
    Map<String, TableOperation> currentOps =
        operationsRepo
            .find(
                dbOperationType, null, tableUuid.orElse(null), databaseName, tableName.orElse(null))
            .stream()
            .filter(e -> e.getTableUuid() != null)
            .map(TableOperation::fromRow)
            .collect(
                Collectors.toMap(
                    TableOperation::getTableUuid, op -> op, TableOperation::mostRecent));

    Map<String, TableOperationsHistory> latestHistory =
        historyRepo.findLatestPerTable(dbOperationType).stream()
            .filter(r -> r.getTableUuid() != null)
            .map(TableOperationsHistory::fromRow)
            .collect(
                Collectors.toMap(
                    TableOperationsHistory::getTableUuid,
                    h -> h,
                    AnalyzerRunner::moreRecentHistory));

    List<Table> tables =
        statsRepo.find(databaseName, tableName.orElse(null), tableUuid.orElse(null)).stream()
            .filter(row -> row.getTableUuid() != null)
            .map(Table::fromRow)
            .collect(Collectors.toList());

    /*
     * For each table in this database, decide whether to create a new PENDING operation.
     *
     * 1. Skip tables not opted in to this operation type. The opt-in check today reads a
     *    table-property flag; in the future it will read a denormalized column.
     * 2. Look up the table's current active operation (if any) and its most recent completed
     *    history entry from the maps loaded above.
     * 3. Delegate the schedule-or-not decision to the analyzer's shouldSchedule — strategy
     *    encapsulates cadence, retry policy, and any future per-operation signals.
     * 4. On true, persist a new PENDING operation. The scheduler picks it up on its next pass.
     */
    tables.forEach(
        table -> {
          if (!analyzer.isEnabled(table)) {
            return;
          }
          Optional<TableOperation> currentOp =
              Optional.ofNullable(currentOps.get(table.getTableUuid()));
          Optional<TableOperationsHistory> entry =
              Optional.ofNullable(latestHistory.get(table.getTableUuid()));
          if (analyzer.shouldSchedule(table, currentOp, entry)) {
            TableOperation op = TableOperation.pending(table, analyzer.getOperationType());
            operationsRepo.save(op.toRow());
            log.info(
                "Created PENDING {} operation for table {}.{}",
                analyzer.getOperationType(),
                table.getDatabaseName(),
                table.getTableId());
          }
        });
  }

  private static TableOperationsHistory moreRecentHistory(
      TableOperationsHistory a, TableOperationsHistory b) {
    Comparator<TableOperationsHistory> byCompletedAt =
        Comparator.comparing(r -> r.getCompletedAt() != null ? r.getCompletedAt() : Instant.EPOCH);
    return byCompletedAt.compare(a, b) >= 0 ? a : b;
  }
}
