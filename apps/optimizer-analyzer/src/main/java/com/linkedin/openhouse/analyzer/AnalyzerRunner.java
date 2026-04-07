package com.linkedin.openhouse.analyzer;

import com.linkedin.openhouse.analyzer.model.Table;
import com.linkedin.openhouse.analyzer.model.TableOperation;
import com.linkedin.openhouse.optimizer.entity.TableOperationHistoryRow;
import com.linkedin.openhouse.optimizer.entity.TableOperationRow;
import com.linkedin.openhouse.optimizer.repository.TableOperationHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

/**
 * Core analysis loop. Loads {@code table_stats} rows and evaluates each table against every
 * registered {@link OperationAnalyzer} in a single pass.
 *
 * <p>The two sides of the join — current operations and circuit-breaker history — are loaded into
 * memory once per run before the table loop. Both are naturally bounded (only tables with active or
 * recently failed operations have rows), so holding them in maps is safe at any table scale.
 *
 * <p>// TODO: Iterate per-database instead of loading all tables at once. This scopes memory usage
 * and allows incremental progress. When we go per-db we may still see 10k tables per iteration, but
 * that should be fine.
 *
 * <p>// TODO: Add benchmarking and scale tests. Measure memory footprint at 10k tables per
 * iteration to validate the in-memory join approach.
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
      String operationType, String databaseName, String tableName, String tableUuid) {

    List<OperationAnalyzer> activeAnalyzers =
        operationType == null
            ? analyzers
            : analyzers.stream()
                .filter(a -> a.getOperationType().equals(operationType))
                .collect(Collectors.toList());

    // Pre-load the small sides of the joins — one query per analyzer type.
    // TODO: Move to a query builder (Criteria API or jOOQ) as filter count grows.
    Map<String, Map<String, TableOperation>> opsByType =
        activeAnalyzers.stream()
            .collect(
                Collectors.toMap(
                    OperationAnalyzer::getOperationType,
                    a ->
                        operationsRepo
                            .find(a.getOperationType(), null, tableUuid, databaseName, tableName)
                            .stream()
                            .filter(e -> e.getTableUuid() != null)
                            .collect(
                                Collectors.toMap(
                                    TableOperationRow::getTableUuid,
                                    TableOperation::from,
                                    TableOperation::mostRecent))));

    Map<String, Map<String, List<TableOperationHistoryRow>>> historyByType =
        activeAnalyzers.stream()
            .collect(
                Collectors.toMap(
                    OperationAnalyzer::getOperationType,
                    a ->
                        historyRepo.find(a.getOperationType(), null, null, null, Pageable.unpaged())
                            .stream()
                            .collect(
                                Collectors.groupingBy(TableOperationHistoryRow::getTableUuid))));

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
                  List<TableOperationHistoryRow> history =
                      historyByType
                          .get(analyzer.getOperationType())
                          .getOrDefault(table.getTableUuid(), Collections.emptyList());
                  Optional<TableOperationHistoryRow> latestHistory = history.stream().findFirst();

                  if (analyzer.shouldSchedule(table, currentOp, latestHistory)
                      && !analyzer.isCircuitBroken(table.getTableUuid(), history)) {
                    TableOperation op = TableOperation.pending(table, analyzer.getOperationType());
                    operationsRepo.save(op.toRow());
                    log.info(
                        "Created PENDING {} operation for table {}.{}",
                        analyzer.getOperationType(),
                        table.getDatabaseId(),
                        table.getTableId());
                  }
                }));

    log.info("Analysis complete");
  }
}
