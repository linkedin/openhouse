package com.linkedin.openhouse.optimizer.service;

import com.linkedin.openhouse.optimizer.db.TableStatsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.HistoryStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
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
  public List<TableOperationDto> listTableOperations(
      Optional<OperationTypeDto> operationType,
      Optional<OperationStatusDto> status,
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid) {
    return operationsRepository
        .find(
            operationType.map(OperationTypeDto::toDb).orElse(null),
            status.map(OperationStatusDto::toDb).orElse(null),
            tableUuid.orElse(null),
            databaseName.orElse(null),
            tableName.orElse(null))
        .stream()
        .map(TableOperationDto::fromRow)
        .collect(Collectors.toList());
  }

  @Override
  @Transactional
  public Optional<TableOperationsHistoryDto> updateOperation(
      String operationId,
      HistoryStatusDto status,
      Long orphanFilesDeleted,
      Long orphanBytesDeleted,
      String errorMessage,
      String errorType) {
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
                    .orphanFilesDeleted(orphanFilesDeleted)
                    .orphanBytesDeleted(orphanBytesDeleted)
                    .errorMessage(errorMessage)
                    .errorType(errorType)
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

    return TableStatsDto.fromRow(saved);
  }

  @Override
  public Optional<TableStatsDto> getTableStats(String tableUuid) {
    return statsRepository.findById(tableUuid).map(TableStatsDto::fromRow);
  }

  @Override
  public List<TableStatsDto> listTableStats(
      Optional<String> databaseName, Optional<String> tableName, Optional<String> tableUuid) {
    return statsRepository
        .find(databaseName.orElse(null), tableName.orElse(null), tableUuid.orElse(null)).stream()
        .map(TableStatsDto::fromRow)
        .collect(Collectors.toList());
  }

  @Override
  public List<TableStatsHistoryDto> getStatsHistory(
      String tableUuid, Optional<Instant> since, int limit) {
    return statsHistoryRepository.find(tableUuid, since.orElse(null), PageRequest.of(0, limit))
        .stream()
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
    return historyRepository
        .findByTableUuidOrderByCompletedAtDesc(tableUuid, PageRequest.of(0, limit)).stream()
        .map(TableOperationsHistoryDto::fromRow)
        .collect(Collectors.toList());
  }
}
