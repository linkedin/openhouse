package com.linkedin.openhouse.analyzer;

import com.linkedin.openhouse.analyzer.model.TableOperationRecord;
import com.linkedin.openhouse.analyzer.model.TableSummary;
import com.linkedin.openhouse.optimizer.entity.TableOperationHistoryRow;
import com.linkedin.openhouse.optimizer.entity.TableOperationRow;
import com.linkedin.openhouse.optimizer.entity.TableStatsRow;
import com.linkedin.openhouse.optimizer.repository.TableOperationHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

/**
 * Core analysis loop. Loads all {@code table_stats} rows and evaluates each table against every
 * registered {@link OperationAnalyzer} in a single pass.
 *
 * <p>The two sides of the join — current operations and circuit-breaker history — are loaded into
 * memory once per run before the table loop. Both are naturally bounded (only tables with active or
 * recently failed operations have rows), so holding them in maps is safe at any table scale.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AnalyzerRunner {

  private final List<OperationAnalyzer> analyzers;
  private final TableStatsRepository statsRepo;
  private final TableOperationsRepository operationsRepo;
  private final TableOperationHistoryRepository historyRepo;

  /** Run the full analysis loop once. */
  public void analyze() {
    // Pre-load the small sides of the joins — one query per analyzer type.
    Map<String, Map<String, TableOperationRecord>> opsByType =
        analyzers.stream()
            .collect(
                Collectors.toMap(
                    OperationAnalyzer::getOperationType, a -> loadOpsMap(a.getOperationType())));

    Map<String, Map<String, List<TableOperationHistoryRow>>> historyByType =
        analyzers.stream()
            .collect(
                Collectors.toMap(
                    OperationAnalyzer::getOperationType,
                    a -> loadHistoryMap(a.getOperationType())));

    List<TableStatsRow> tableList =
        statsRepo.find(null, null, null).stream()
            .filter(row -> row.getTableUuid() != null)
            .collect(Collectors.toList());
    log.info("Found {} tables in optimizer table_stats", tableList.size());

    tableList.forEach(
        row -> {
          TableSummary table = toSummary(row);
          analyzers.forEach(
              analyzer -> {
                String type = analyzer.getOperationType();
                Optional<TableOperationRecord> currentOp =
                    Optional.ofNullable(opsByType.get(type).get(row.getTableUuid()));
                List<TableOperationHistoryRow> history =
                    historyByType
                        .get(type)
                        .getOrDefault(row.getTableUuid(), Collections.emptyList());

                Optional<TableOperationHistoryRow> latestHistory = history.stream().findFirst();

                if (analyzer.isEnabled(table)
                    && analyzer.shouldSchedule(table, currentOp, latestHistory)
                    && !isCircuitBroken(analyzer, row.getTableUuid(), history)) {
                  operationsRepo.save(buildOperation(row, type));
                  log.info(
                      "Created PENDING {} operation for table {}.{}",
                      type,
                      row.getDatabaseId(),
                      row.getTableName());
                }
              });
        });

    log.info("Analysis complete");
  }

  /**
   * Loads the most recent operation record per table for the given type. Deduplicates by keeping
   * the newer row when a table has more than one active record.
   */
  private Map<String, TableOperationRecord> loadOpsMap(String operationType) {
    Map<String, TableOperationRecord> map =
        operationsRepo.find(operationType, null, null, null, null).stream()
            .filter(e -> e.getTableUuid() != null)
            .collect(
                Collectors.toMap(
                    TableOperationRow::getTableUuid,
                    AnalyzerRunner::toRecord,
                    (a, b) -> mostRecent(a, b)));
    log.info("Analyzer {} found {} tables with operation history", operationType, map.size());
    return map;
  }

  /**
   * Loads all history rows for the given type and groups them by {@code tableUuid}, newest first.
   * Called once per analyzer type to eliminate per-table N+1 queries in the circuit breaker check.
   */
  private Map<String, List<TableOperationHistoryRow>> loadHistoryMap(String operationType) {
    return historyRepo.find(operationType, null, null, null, Pageable.unpaged()).stream()
        .collect(Collectors.groupingBy(TableOperationHistoryRow::getTableUuid));
  }

  private TableOperationRow buildOperation(TableStatsRow row, String operationType) {
    return TableOperationRow.builder()
        .id(UUID.randomUUID().toString())
        .tableUuid(row.getTableUuid())
        .databaseName(row.getDatabaseId())
        .tableName(row.getTableName())
        .operationType(operationType)
        .status("PENDING")
        .createdAt(Instant.now())
        .version(0L)
        .build();
  }

  private TableSummary toSummary(TableStatsRow e) {
    return TableSummary.builder()
        .tableUuid(e.getTableUuid())
        .databaseId(e.getDatabaseId())
        .tableId(e.getTableName())
        .tableProperties(
            e.getTableProperties() != null ? e.getTableProperties() : Collections.emptyMap())
        .stats(e.getStats())
        .build();
  }

  /**
   * Returns {@code true} if the circuit breaker has tripped. Uses the pre-loaded history list
   * instead of querying the DB per table.
   */
  private boolean isCircuitBroken(
      OperationAnalyzer analyzer, String tableUuid, List<TableOperationHistoryRow> history) {
    int threshold = analyzer.getCircuitBreakerThreshold();
    if (threshold <= 0 || history.size() < threshold) {
      return false;
    }
    boolean allFailed =
        history.stream().limit(threshold).allMatch(r -> "FAILED".equals(r.getStatus()));
    if (allFailed) {
      log.warn(
          "Circuit breaker tripped for table {} operation {}: last {} attempts all FAILED",
          tableUuid,
          analyzer.getOperationType(),
          threshold);
    }
    return allFailed;
  }

  private static TableOperationRecord mostRecent(TableOperationRecord a, TableOperationRecord b) {
    Comparator<TableOperationRecord> byCreatedAt =
        Comparator.comparing(r -> r.getCreatedAt() != null ? r.getCreatedAt() : Instant.EPOCH);
    return byCreatedAt.compare(a, b) >= 0 ? a : b;
  }

  private static TableOperationRecord toRecord(TableOperationRow e) {
    TableOperationRecord r = new TableOperationRecord();
    r.setId(e.getId());
    r.setTableUuid(e.getTableUuid());
    r.setDatabaseName(e.getDatabaseName());
    r.setTableName(e.getTableName());
    r.setOperationType(e.getOperationType());
    r.setStatus(e.getStatus());
    r.setCreatedAt(e.getCreatedAt());
    r.setScheduledAt(e.getScheduledAt());
    return r;
  }
}
