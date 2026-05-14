package com.linkedin.openhouse.optimizer.service;

import com.linkedin.openhouse.optimizer.db.TableOperationsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableStatsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.HistoryStatus;
import com.linkedin.openhouse.optimizer.model.OperationStatus;
import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.Table;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistory;
import com.linkedin.openhouse.optimizer.model.TableStatsHistory;
import com.linkedin.openhouse.optimizer.model.mapper.ModelDbMapper;
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
 * <p>Operates purely on model/ and db/ types. The model↔db boundary is the {@link ModelDbMapper}.
 * No api/-package types appear in this class.
 */
@Service
@RequiredArgsConstructor
public class OptimizerDataServiceImpl implements OptimizerDataService {

  private final TableOperationsRepository operationsRepository;
  private final TableOperationsHistoryRepository historyRepository;
  private final TableStatsRepository statsRepository;
  private final TableStatsHistoryRepository statsHistoryRepository;
  private final ModelDbMapper dbMapper;

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
            operationType.map(dbMapper::toDbOperationType).orElse(null),
            status.map(dbMapper::toDbOperationStatus).orElse(null),
            tableUuid.orElse(null),
            databaseName.orElse(null),
            tableName.orElse(null))
        .stream()
        .map(dbMapper::toOperation)
        .collect(Collectors.toList());
  }

  @Override
  @Transactional
  public Optional<TableOperationsHistory> completeOperation(
      String operationId, HistoryStatus status) {
    return operationsRepository
        .findById(operationId)
        .map(
            row -> {
              TableOperationsHistoryRow historyRow =
                  TableOperationsHistoryRow.builder()
                      .id(row.getId())
                      .tableUuid(row.getTableUuid())
                      .databaseName(row.getDatabaseName())
                      .tableName(row.getTableName())
                      .operationType(row.getOperationType())
                      .completedAt(Instant.now())
                      .status(dbMapper.toDbHistoryStatus(status))
                      .build();
              return dbMapper.toHistory(historyRepository.save(historyRow));
            });
  }

  @Override
  public Optional<TableOperation> getTableOperation(String id) {
    return operationsRepository.findById(id).map(dbMapper::toOperation);
  }

  // --- TableStats ---

  @Override
  @Transactional
  public Table upsertTableStats(Table table) {
    Instant now = Instant.now();
    String tableUuid = table.getTableUuid();

    TableStatsRow row =
        statsRepository
            .findById(tableUuid)
            .map(
                existing ->
                    existing
                        .toBuilder()
                        .databaseName(table.getDatabaseName())
                        .tableName(table.getTableId())
                        .snapshot(dbMapper.toDbSnapshot(table.getStats()))
                        .tableProperties(table.getTableProperties())
                        .updatedAt(now)
                        .build())
            .orElse(
                TableStatsRow.builder()
                    .tableUuid(tableUuid)
                    .databaseName(table.getDatabaseName())
                    .tableName(table.getTableId())
                    .snapshot(dbMapper.toDbSnapshot(table.getStats()))
                    .tableProperties(table.getTableProperties())
                    .updatedAt(now)
                    .build());
    TableStatsRow saved = statsRepository.save(row);

    statsHistoryRepository.save(
        TableStatsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(tableUuid)
            .databaseName(table.getDatabaseName())
            .tableName(table.getTableId())
            .snapshot(dbMapper.toDbSnapshot(table.getStats()))
            .delta(dbMapper.toDbDelta(table.getStats()))
            .recordedAt(now)
            .build());

    return dbMapper.toTable(saved);
  }

  @Override
  public Optional<Table> getTableStats(String tableUuid) {
    return statsRepository.findById(tableUuid).map(dbMapper::toTable);
  }

  @Override
  public List<Table> listTableStats(
      Optional<String> databaseName, Optional<String> tableName, Optional<String> tableUuid) {
    return statsRepository
        .find(databaseName.orElse(null), tableName.orElse(null), tableUuid.orElse(null)).stream()
        .map(dbMapper::toTable)
        .collect(Collectors.toList());
  }

  @Override
  public List<TableStatsHistory> getStatsHistory(
      String tableUuid, Optional<Instant> since, int limit) {
    return statsHistoryRepository.find(tableUuid, since.orElse(null), PageRequest.of(0, limit))
        .stream()
        .map(dbMapper::toStatsHistory)
        .collect(Collectors.toList());
  }

  // --- TableOperationsHistory ---

  @Override
  @Transactional
  public TableOperationsHistory appendHistory(TableOperationsHistory history) {
    TableOperationsHistoryRow row =
        TableOperationsHistoryRow.builder()
            .id(history.getId())
            .tableUuid(history.getTableUuid())
            .databaseName(history.getDatabaseName())
            .tableName(history.getTableName())
            .operationType(dbMapper.toDbOperationType(history.getOperationType()))
            .completedAt(
                history.getCompletedAt() != null ? history.getCompletedAt() : Instant.now())
            .status(dbMapper.toDbHistoryStatus(history.getStatus()))
            .build();
    return dbMapper.toHistory(historyRepository.save(row));
  }

  @Override
  public List<TableOperationsHistory> getHistory(String tableUuid, int limit) {
    return historyRepository
        .findByTableUuidOrderByCompletedAtDesc(tableUuid, PageRequest.of(0, limit)).stream()
        .map(dbMapper::toHistory)
        .collect(Collectors.toList());
  }
}
