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
 * Core analysis loop. Loads {@code table_stats} rows and evaluates each table against every
 * registered {@link OperationAnalyzer} in a single pass.
 *
 * <p>Both sides of the join — current operations and latest history per (table, type) — are loaded
 * into maps once per run before the table loop. This is correct at small scale (≤~100k tables);
 * past that the runner OOMs and exceeds the cadence window. Scale-up work (per-op enablement
 * column, cooldown anti-join push-down, per-db iteration, streaming reads, batched writes, rate
 * limiting) is tracked in <a
 * href="https://linkedin.atlassian.net/browse/BDP-102182">BDP-102182</a>.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AnalyzerRunner {

  private final List<OperationAnalyzer> analyzers;
  private final TableStatsRepository statsRepo;
  private final TableOperationsRepository operationsRepo;
  private final TableOperationHistoryRepository historyRepo;

  /** Run the full analysis loop once with no filters. */
  public void analyze() {
    analyze(null, null, null, null);
  }

  /**
   * Run the analysis loop, optionally scoped to a specific operation type, database, table name, or
   * table UUID. Pass {@code null} for any parameter to skip that filter.
   */
  public void analyze(
      OperationType operationType, String databaseName, String tableName, String tableUuid) {

    List<OperationAnalyzer> activeAnalyzers =
        operationType == null
            ? analyzers
            : analyzers.stream()
                .filter(a -> a.getOperationType() == operationType)
                .collect(Collectors.toList());

    // Pre-load the small sides of the joins — one query per analyzer type.
    // TODO: Move to a query builder (Criteria API or jOOQ) as filter count grows.
    Map<OperationType, Map<String, TableOperation>> opsByType =
        activeAnalyzers.stream()
            .collect(
                Collectors.toMap(
                    OperationAnalyzer::getOperationType,
                    a ->
                        operationsRepo
                            .find(
                                a.getOperationType().name(),
                                null,
                                tableUuid,
                                databaseName,
                                tableName)
                            .stream()
                            .filter(e -> e.getTableUuid() != null)
                            .collect(
                                Collectors.toMap(
                                    TableOperationRow::getTableUuid,
                                    TableOperation::from,
                                    TableOperation::mostRecent))));

    // Latest history row per (table_uuid, operation_type), one query per analyzer. The repo query
    // may return tied rows for the same key on identical completed_at; dedupe in memory.
    Map<OperationType, Map<String, TableOperationHistoryRow>> latestHistoryByType =
        activeAnalyzers.stream()
            .collect(
                Collectors.toMap(
                    OperationAnalyzer::getOperationType,
                    a ->
                        historyRepo.findLatestPerTable(a.getOperationType().name()).stream()
                            .filter(r -> r.getTableUuid() != null)
                            .collect(
                                Collectors.toMap(
                                    TableOperationHistoryRow::getTableUuid,
                                    r -> r,
                                    AnalyzerRunner::moreRecentHistory))));

    List<Table> tables =
        statsRepo.find(databaseName, tableName, tableUuid).stream()
            .filter(row -> row.getTableUuid() != null)
            .map(Table::from)
            .collect(Collectors.toList());
    log.info("Found {} tables in optimizer table_stats", tables.size());

    tables.forEach(
        table ->
            activeAnalyzers.forEach(
                analyzer -> {
                  if (!analyzer.isEnabled(table)) {
                    return;
                  }

                  Optional<TableOperation> currentOp =
                      Optional.ofNullable(
                          opsByType.get(analyzer.getOperationType()).get(table.getTableUuid()));
                  Optional<TableOperationHistoryRow> latestHistory =
                      Optional.ofNullable(
                          latestHistoryByType
                              .get(analyzer.getOperationType())
                              .get(table.getTableUuid()));

                  if (analyzer.shouldSchedule(table, currentOp, latestHistory)) {
                    TableOperation op = TableOperation.pending(table, analyzer.getOperationType());
                    operationsRepo.save(op.toRow());
                    log.info(
                        "Created PENDING {} operation for table {}.{}",
                        analyzer.getOperationType(),
                        table.getDatabaseName(),
                        table.getTableId());
                  }
                }));

    log.info("Analysis complete");
  }

  private static TableOperationHistoryRow moreRecentHistory(
      TableOperationHistoryRow a, TableOperationHistoryRow b) {
    Comparator<TableOperationHistoryRow> byCompletedAt =
        Comparator.comparing(r -> r.getCompletedAt() != null ? r.getCompletedAt() : Instant.EPOCH);
    return byCompletedAt.compare(a, b) >= 0 ? a : b;
  }
}
