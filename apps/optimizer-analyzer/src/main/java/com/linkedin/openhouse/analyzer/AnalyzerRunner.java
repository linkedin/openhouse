package com.linkedin.openhouse.analyzer;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

/**
 * Core analysis loop. For one operation type per call, iterates databases and evaluates each table
 * in a database against the matching {@link OperationAnalyzer}.
 *
 * <p>Both sides of the join — current operations and latest history per (table, type) — are loaded
 * into maps once per database before the table loop. This is correct at small scale (≤~100k
 * tables); past that the per-db query shape and projection need further tuning. Scale-up work is
 * tracked in <a href="https://linkedin.atlassian.net/browse/BDP-102182">BDP-102182</a>.
 *
 * <p>// TODO(scale-test): benchmark the per-db working set at up to 10k tables and measure JVM heap
 * residency for the three intermediate maps; per-db iteration bounds memory by tables-per-db rather
 * than tables-total, but the upper bound still needs empirical validation.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AnalyzerRunner {

  private final List<OperationAnalyzer> analyzers;
  private final TableStatsRepository statsRepo;
  private final TableOperationsRepository operationsRepo;
  private final TableOperationsHistoryRepository historyRepo;

  @Value("${optimizer.repo.default-limit:10000}")
  private int defaultLimit = 10_000;

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

  private void analyzeDatabase(
      OperationAnalyzer analyzer,
      String databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid) {

    com.linkedin.openhouse.optimizer.db.OperationType dbOperationType =
        analyzer.getOperationType().toDb();

    // Pre-load the small sides of the joins — bounded by tables in this database.
    PageRequest page = PageRequest.of(0, defaultLimit);
    Map<String, TableOperationDto> currentOps =
        operationsRepo
            .find(
                Optional.of(dbOperationType),
                Optional.empty(),
                tableUuid,
                Optional.of(databaseName),
                tableName,
                Optional.empty(),
                Optional.empty(),
                page)
            .stream()
            .filter(e -> e.getTableUuid() != null)
            .map(TableOperationDto::fromRow)
            .collect(
                Collectors.toMap(
                    TableOperationDto::getTableUuid, op -> op, TableOperationDto::mostRecent));

    Map<String, TableOperationsHistoryDto> latestHistory =
        historyRepo.findLatest(dbOperationType, page).stream()
            .filter(r -> r.getTableUuid() != null)
            .map(TableOperationsHistoryDto::fromRow)
            .collect(
                Collectors.toMap(
                    TableOperationsHistoryDto::getTableUuid,
                    h -> h,
                    AnalyzerRunner::moreRecentHistory));

    List<TableDto> tables =
        statsRepo.find(Optional.of(databaseName), tableName, tableUuid, page).stream()
            .filter(row -> row.getTableUuid() != null)
            .map(TableDto::fromRow)
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
          Optional<TableOperationDto> currentOp =
              Optional.ofNullable(currentOps.get(table.getTableUuid()));
          Optional<TableOperationsHistoryDto> entry =
              Optional.ofNullable(latestHistory.get(table.getTableUuid()));
          if (analyzer.shouldSchedule(table, currentOp, entry)) {
            TableOperationDto op = TableOperationDto.pending(table, analyzer.getOperationType());
            operationsRepo.save(op.toRow());
            log.info(
                "Created PENDING {} operation for table {}.{}",
                analyzer.getOperationType(),
                table.getDatabaseName(),
                table.getTableId());
          }
        });
  }

  private static TableOperationsHistoryDto moreRecentHistory(
      TableOperationsHistoryDto a, TableOperationsHistoryDto b) {
    Comparator<TableOperationsHistoryDto> byCompletedAt =
        Comparator.comparing(r -> r.getCompletedAt() != null ? r.getCompletedAt() : Instant.EPOCH);
    return byCompletedAt.compare(a, b) >= 0 ? a : b;
  }
}
