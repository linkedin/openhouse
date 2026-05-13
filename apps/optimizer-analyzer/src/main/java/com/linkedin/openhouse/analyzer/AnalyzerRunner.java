package com.linkedin.openhouse.analyzer;

import com.linkedin.openhouse.analyzer.model.OperationType;
import com.linkedin.openhouse.analyzer.model.Table;
import com.linkedin.openhouse.analyzer.model.TableOperation;
import com.linkedin.openhouse.optimizer.entity.TableOperationHistoryRow;
import com.linkedin.openhouse.optimizer.entity.TableOperationRow;
import com.linkedin.openhouse.optimizer.repository.TableOperationHistoryRepository;
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
  private final TableOperationHistoryRepository historyRepo;

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

    String operationType = analyzer.getOperationType().name();

    // Pre-load the small sides of the joins — bounded by tables in this database.
    Map<String, TableOperation> currentOps =
        operationsRepo
            .find(operationType, null, tableUuid.orElse(null), databaseName, tableName.orElse(null))
            .stream()
            .filter(e -> e.getTableUuid() != null)
            .collect(
                Collectors.toMap(
                    TableOperationRow::getTableUuid,
                    TableOperation::from,
                    TableOperation::mostRecent));

    // Latest history row per (table_uuid, op_type) for this analyzer. The repo query may return
    // tied rows on identical completed_at; dedupe in memory.
    Map<String, TableOperationHistoryRow> latestHistory =
        historyRepo.findLatestPerTable(operationType).stream()
            .filter(r -> r.getTableUuid() != null)
            .collect(
                Collectors.toMap(
                    TableOperationHistoryRow::getTableUuid,
                    r -> r,
                    AnalyzerRunner::moreRecentHistory));

    List<Table> tables =
        statsRepo.find(databaseName, tableName.orElse(null), tableUuid.orElse(null)).stream()
            .filter(row -> row.getTableUuid() != null)
            .map(Table::from)
            .collect(Collectors.toList());

    tables.forEach(
        table -> {
          if (!analyzer.isEnabled(table)) {
            return;
          }
          Optional<TableOperation> currentOp =
              Optional.ofNullable(currentOps.get(table.getTableUuid()));
          Optional<TableOperationHistoryRow> entry =
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

  private static TableOperationHistoryRow moreRecentHistory(
      TableOperationHistoryRow a, TableOperationHistoryRow b) {
    Comparator<TableOperationHistoryRow> byCompletedAt =
        Comparator.comparing(r -> r.getCompletedAt() != null ? r.getCompletedAt() : Instant.EPOCH);
    return byCompletedAt.compare(a, b) >= 0 ? a : b;
  }
}
