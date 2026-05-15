package com.linkedin.openhouse.optimizer.service;

import com.linkedin.openhouse.optimizer.db.TableStatsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.HistoryStatus;
import com.linkedin.openhouse.optimizer.model.OperationStatus;
import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistory;
import com.linkedin.openhouse.optimizer.model.TableStats;
import com.linkedin.openhouse.optimizer.model.TableStatsHistory;
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
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Implementation of {@link OptimizerDataService}.
 *
 * <p>Operates purely on model/ and db/ types. Conversion happens via the {@code toRow()} / {@code
 * fromRow(...)} methods on the model types themselves — no injected mapper. No api/-package types
 * appear in this class.
 */
@Service
@RequiredArgsConstructor
public class OptimizerDataServiceImpl implements OptimizerDataService {

  private final TableOperationsRepository operationsRepository;
  private final TableOperationsHistoryRepository historyRepository;
  private final TableStatsRepository statsRepository;
  private final TableStatsHistoryRepository statsHistoryRepository;

  // --- TableOperations ---

  @Override
  public List<TableOperation> listTableOperations(
      Optional<OperationType> operationType,
      Optional<OperationStatus> status,
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid) {
    return operationsRepository
        .find(
            operationType.map(OperationType::toDb).orElse(null),
            status.map(OperationStatus::toDb).orElse(null),
            tableUuid.orElse(null),
            databaseName.orElse(null),
            tableName.orElse(null))
        .stream()
        .map(TableOperation::fromRow)
        .collect(Collectors.toList());
  }

  @Override
  @Transactional
  public Optional<TableOperationsHistory> completeOperation(
      String operationId, HistoryStatus status) {
    return operationsRepository
        .findById(operationId)
        .map(
            row ->
                TableOperationsHistory.builder()
                    .id(row.getId())
                    .tableUuid(row.getTableUuid())
                    .databaseName(row.getDatabaseName())
                    .tableName(row.getTableName())
                    .operationType(OperationType.fromDb(row.getOperationType()))
                    .completedAt(Instant.now())
                    .status(status)
                    .build())
        .map(history -> TableOperationsHistory.fromRow(historyRepository.save(history.toRow())));
  }

  @Override
  public Optional<TableOperation> getTableOperation(String id) {
    return operationsRepository.findById(id).map(TableOperation::fromRow);
  }

  // --- TableStats ---

  @Override
  @Transactional
  public TableStats upsertTableStats(TableStats stats) {
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
    TableStatsRow saved = statsRepository.save(row);

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

    return TableStats.fromRow(saved);
  }

  @Override
  public Optional<TableStats> getTableStats(String tableUuid) {
    return statsRepository.findById(tableUuid).map(TableStats::fromRow);
  }

  @Override
  public List<TableStats> listTableStats(
      Optional<String> databaseName, Optional<String> tableName, Optional<String> tableUuid) {
    return statsRepository
        .find(databaseName.orElse(null), tableName.orElse(null), tableUuid.orElse(null)).stream()
        .map(TableStats::fromRow)
        .collect(Collectors.toList());
  }

  @Override
  public List<TableStatsHistory> getStatsHistory(
      String tableUuid, Optional<Instant> since, int limit) {
    return statsHistoryRepository.find(tableUuid, since.orElse(null), PageRequest.of(0, limit))
        .stream()
        .map(TableStatsHistory::fromRow)
        .collect(Collectors.toList());
  }

  // --- TableOperationsHistory ---

  @Override
  @Transactional
  public TableOperationsHistory appendHistory(TableOperationsHistory history) {
    TableOperationsHistory toWrite =
        history
            .toBuilder()
            .completedAt(
                history.getCompletedAt() != null ? history.getCompletedAt() : Instant.now())
            .build();
    return TableOperationsHistory.fromRow(historyRepository.save(toWrite.toRow()));
  }

  @Override
  public List<TableOperationsHistory> getHistory(String tableUuid, int limit) {
    return historyRepository
        .findByTableUuidOrderByCompletedAtDesc(tableUuid, PageRequest.of(0, limit)).stream()
        .map(TableOperationsHistory::fromRow)
        .collect(Collectors.toList());
  }
}
