package com.linkedin.openhouse.optimizer.service;

import com.linkedin.openhouse.optimizer.analyzer.AnalyzeRequest;
import com.linkedin.openhouse.optimizer.analyzer.AnalyzerRunner;
import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.SnapshotMetrics;
import com.linkedin.openhouse.optimizer.db.TableOperationsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
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
 * <p>Operates only on model/ and db/ types. Persistence rows own conversion to and from the
 * Spring-free optimizer model; no injected mapper or api/-package type appears here.
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
            operationType.map(OperationType::fromModel),
            status.map(OperationStatus::fromModel),
            tableUuid,
            databaseName,
            tableName,
            Optional.empty(),
            Optional.empty(),
            PageRequest.of(0, limit))
        .stream()
        .map(TableOperationsRow::toModel)
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
                    .operationType(
                        row.getOperationType() == null ? null : row.getOperationType().toModel())
                    .completedAt(Instant.now())
                    .status(status)
                    .build())
        .map(
            history ->
                historyRepository.save(TableOperationsHistoryRow.fromModel(history)).toModel());
  }

  @Override
  public Optional<TableOperationDto> getTableOperation(String id) {
    return operationsRepository.findById(id).map(TableOperationsRow::toModel);
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
                        .snapshot(SnapshotMetrics.fromModel(stats.getSnapshot()))
                        .tableProperties(stats.getTableProperties())
                        .updatedAt(now)
                        .build())
            .orElse(TableStatsRow.fromModel(stats.toBuilder().updatedAt(now).build()));
    // 1. Update the current per-table stats in MySQL (one row per table, upserted in place).
    TableStatsRow saved = statsRepository.save(row);

    // 2. Append this commit's stats to the historical stats table. History starts with a short
    //    retention (4 days). In the future we may add an aggregate table holding stats rolled up
    //    over multiple days; that aggregation path could be a streaming job or a MySQL query. It is
    //    not needed now and will be decided when required.
    statsHistoryRepository.save(
        TableStatsHistoryRow.fromModel(
            TableStatsHistoryDto.builder()
                .id(UUID.randomUUID().toString())
                .tableUuid(tableUuid)
                .databaseName(stats.getDatabaseName())
                .tableName(stats.getTableName())
                .stats(stats)
                .recordedAt(now)
                .build()));

    // 3. Non-blocking trigger of commit-driven analysis as upsertTableStats does not need response
    //    from analyze, reusing the in-memory stats (no re-read).
    triggerCommitDrivenAnalysis(saved.toTableModel());

    return saved.toModel();
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
    return statsRepository.findById(tableUuid).map(TableStatsRow::toModel);
  }

  @Override
  public List<TableStatsDto> listTableStats(
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid,
      int limit) {
    return statsRepository.find(databaseName, tableName, tableUuid, PageRequest.of(0, limit))
        .stream()
        .map(TableStatsRow::toModel)
        .collect(Collectors.toList());
  }

  @Override
  public List<TableStatsHistoryDto> getStatsHistory(
      String tableUuid, Optional<Instant> since, int limit) {
    return statsHistoryRepository.find(tableUuid, since, PageRequest.of(0, limit)).stream()
        .map(TableStatsHistoryRow::toModel)
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
    return historyRepository.save(TableOperationsHistoryRow.fromModel(toWrite)).toModel();
  }

  @Override
  public List<TableOperationsHistoryDto> getHistory(String tableUuid, int limit) {
    return historyRepository.find(tableUuid, PageRequest.of(0, limit)).stream()
        .map(TableOperationsHistoryRow::toModel)
        .collect(Collectors.toList());
  }
}
