package com.linkedin.openhouse.optimizer.service;

import com.linkedin.openhouse.optimizer.analyzer.AnalyzeRequest;
import com.linkedin.openhouse.optimizer.analyzer.AnalyzerRunner;
import com.linkedin.openhouse.optimizer.db.TableStatsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.HistoryStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import com.linkedin.openhouse.optimizer.model.TableStatsHistoryDto;
import com.linkedin.openhouse.optimizer.repository.TableOperationsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Implementation of {@link OptimizerDataService}.
 *
 * <p>Operates purely on model/ and db/ types. Conversion happens via the {@code toRow()} / {@code
 * fromRow(...)} methods on the model types themselves — no injected mapper. No api/-package types
 * appear in this class.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class OptimizerDataServiceImpl implements OptimizerDataService {

  private final TableOperationsRepository operationsRepository;
  private final TableOperationsHistoryRepository historyRepository;
  private final TableStatsRepository statsRepository;
  private final TableStatsHistoryRepository statsHistoryRepository;
  private final AnalyzerRunner analyzerRunner;

  // --- TableOperations ---

  @Override
  public List<TableOperationDto> listTableOperations(
      Optional<OperationTypeDto> operationType,
      Optional<OperationStatusDto> status,
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid,
      int limit) {
    return operationsRepository
        .find(
            operationType.map(OperationTypeDto::toDb),
            status.map(OperationStatusDto::toDb),
            tableUuid,
            databaseName,
            tableName,
            Optional.empty(),
            Optional.empty(),
            PageRequest.of(0, limit))
        .stream()
        .map(TableOperationDto::fromRow)
        .collect(Collectors.toList());
  }

  @Override
  @Transactional
  public Optional<TableOperationsHistoryDto> updateOperation(
      String operationId, HistoryStatusDto status) {
    return operationsRepository
        .findById(operationId)
        .map(
            row ->
                TableOperationsHistoryDto.builder()
                    .id(row.getId())
                    .tableUuid(row.getTableUuid())
                    .databaseName(row.getDatabaseName())
                    .tableName(row.getTableName())
                    .operationType(OperationTypeDto.fromDb(row.getOperationType()))
                    .completedAt(Instant.now())
                    .status(status)
                    .build())
        .map(history -> TableOperationsHistoryDto.fromRow(historyRepository.save(history.toRow())));
  }

  @Override
  public Optional<TableOperationDto> getTableOperation(String id) {
    return operationsRepository.findById(id).map(TableOperationDto::fromRow);
  }

  // --- TableStatsDto ---

  @Override
  @Transactional
  public TableStatsDto upsertTableStats(TableStatsDto stats) {
    Instant now = Instant.now();
    String tableUuid = stats.getTableUuid();

    TableStatsRow row =
        statsRepository
            .findById(tableUuid)
            .map(
                existing ->
                    existing
                        .toBuilder()
                        .databaseName(stats.getDatabaseName())
                        .tableName(stats.getTableName())
                        .snapshot(stats.toSnapshotRow())
                        .tableProperties(stats.getTableProperties())
                        .updatedAt(now)
                        .build())
            .orElse(stats.toBuilder().updatedAt(now).build().toRow());
    // 1. Update the current per-table stats in MySQL (one row per table, upserted in place).
    TableStatsRow saved = statsRepository.save(row);

    // 2. Append this commit's stats to the historical stats table. History starts with a short
    //    retention (4 days). In the future we may add an aggregate table holding stats rolled up
    //    over multiple days; that aggregation path could be a streaming job or a MySQL query. It is
    //    not needed now and will be decided when required.
    statsHistoryRepository.save(
        TableStatsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(tableUuid)
            .databaseName(stats.getDatabaseName())
            .tableName(stats.getTableName())
            .snapshot(stats.toSnapshotRow())
            .delta(stats.toDeltaRow())
            .recordedAt(now)
            .build());

    // 3. Non-blocking trigger of commit-driven analysis as upsertTableStats does not need response
    //    from analyze, reusing the in-memory stats (no re-read).
    triggerCommitDrivenAnalysis(TableDto.fromRow(saved));

    return TableStatsDto.fromRow(saved);
  }

  /**
   * Best-effort, non-blocking commit-driven analysis for the just-committed table, using the stats
   * already in memory ({@link TableDto} built from the saved row) so the analyzer does not re-read
   * {@code table_stats}. The work is handed to a bounded-elastic worker and the caller returns
   * immediately, so the stats upsert never waits on analysis. Failures are logged and swallowed:
   * analysis is an optimization and the periodic full scan reconciles anything a transient failure
   * skips — so a trigger failure must never fail the stats upsert.
   */
  private void triggerCommitDrivenAnalysis(TableDto table) {
    Mono.fromRunnable(() -> analyzerRunner.analyze(AnalyzeRequest.builder().table(table).build()))
        .subscribeOn(Schedulers.boundedElastic())
        .doOnError(
            e ->
                log.warn(
                    "Commit-driven analysis trigger failed for {}.{} (uuid={}); skipping",
                    table.getDatabaseName(),
                    table.getTableId(),
                    table.getTableUuid(),
                    e))
        .onErrorComplete()
        .subscribe();
  }

  @Override
  public Optional<TableStatsDto> getTableStats(String tableUuid) {
    return statsRepository.findById(tableUuid).map(TableStatsDto::fromRow);
  }

  @Override
  public List<TableStatsDto> listTableStats(
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid,
      int limit) {
    return statsRepository.find(databaseName, tableName, tableUuid, PageRequest.of(0, limit))
        .stream()
        .map(TableStatsDto::fromRow)
        .collect(Collectors.toList());
  }

  @Override
  public List<TableStatsHistoryDto> getStatsHistory(
      String tableUuid, Optional<Instant> since, int limit) {
    return statsHistoryRepository.find(tableUuid, since, PageRequest.of(0, limit)).stream()
        .map(TableStatsHistoryDto::fromRow)
        .collect(Collectors.toList());
  }

  // --- TableOperationsHistoryDto ---

  @Override
  @Transactional
  public TableOperationsHistoryDto appendHistory(TableOperationsHistoryDto history) {
    TableOperationsHistoryDto toWrite =
        history
            .toBuilder()
            .completedAt(
                history.getCompletedAt() != null ? history.getCompletedAt() : Instant.now())
            .build();
    return TableOperationsHistoryDto.fromRow(historyRepository.save(toWrite.toRow()));
  }

  @Override
  public List<TableOperationsHistoryDto> getHistory(String tableUuid, int limit) {
    return historyRepository.find(tableUuid, PageRequest.of(0, limit)).stream()
        .map(TableOperationsHistoryDto::fromRow)
        .collect(Collectors.toList());
  }
}
