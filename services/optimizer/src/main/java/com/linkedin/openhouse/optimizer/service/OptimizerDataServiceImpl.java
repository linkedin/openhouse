package com.linkedin.openhouse.optimizer.service;

import com.linkedin.openhouse.optimizer.api.mapper.OptimizerMapper;
import com.linkedin.openhouse.optimizer.api.model.CompleteOperationRequest;
import com.linkedin.openhouse.optimizer.api.model.OperationHistoryStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsDto;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.api.model.TableStatsDto;
import com.linkedin.openhouse.optimizer.api.model.TableStatsHistoryDto;
import com.linkedin.openhouse.optimizer.api.model.UpsertTableStatsRequest;
import com.linkedin.openhouse.optimizer.entity.TableOperationsHistoryRow;
import com.linkedin.openhouse.optimizer.entity.TableStatsHistoryRow;
import com.linkedin.openhouse.optimizer.entity.TableStatsRow;
import com.linkedin.openhouse.optimizer.repository.TableOperationsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Implementation of {@link OptimizerDataService}. */
@Service
@RequiredArgsConstructor
public class OptimizerDataServiceImpl implements OptimizerDataService {

  private final TableOperationsRepository operationsRepository;
  private final TableOperationsHistoryRepository historyRepository;
  private final TableStatsRepository statsRepository;
  private final TableStatsHistoryRepository statsHistoryRepository;
  private final OptimizerMapper mapper;

  // --- TableOperations ---

  @Override
  public List<TableOperationsDto> listTableOperations(
      OperationType operationType,
      OperationStatus status,
      String databaseName,
      String tableName,
      String tableUuid) {
    return operationsRepository.find(operationType, status, databaseName, tableName, tableUuid)
        .stream()
        .map(mapper::toDto)
        .collect(Collectors.toList());
  }

  @Override
  @Transactional
  public Optional<TableOperationsHistoryDto> completeOperation(
      String id, CompleteOperationRequest request) {
    return operationsRepository
        .findById(id)
        .map(
            row -> {
              TableOperationsHistoryRow historyRow =
                  TableOperationsHistoryRow.builder()
                      .id(row.getId())
                      .tableUuid(row.getTableUuid())
                      .databaseName(row.getDatabaseName())
                      .tableName(row.getTableName())
                      .operationType(row.getOperationType())
                      .submittedAt(Instant.now())
                      .status(request.getStatus())
                      .jobId(row.getJobId())
                      .result(request.getResult())
                      .build();
              return mapper.toDto(historyRepository.save(historyRow));
            });
  }

  @Override
  public Optional<TableOperationsDto> getTableOperation(String id) {
    return operationsRepository.findById(id).map(mapper::toDto);
  }

  // --- TableStats ---

  @Override
  @Transactional
  public TableStatsDto upsertTableStats(String tableUuid, UpsertTableStatsRequest request) {
    Instant now = Instant.now();
    TableStatsRow row =
        statsRepository
            .findById(tableUuid)
            .map(
                existing ->
                    existing
                        .toBuilder()
                        .databaseId(request.getDatabaseId())
                        .tableName(request.getTableName())
                        .stats(request.getStats())
                        .tableProperties(request.getTableProperties())
                        .updatedAt(now)
                        .build())
            .orElse(
                TableStatsRow.builder()
                    .tableUuid(tableUuid)
                    .databaseId(request.getDatabaseId())
                    .tableName(request.getTableName())
                    .stats(request.getStats())
                    .tableProperties(request.getTableProperties())
                    .updatedAt(now)
                    .build());
    TableStatsDto saved = mapper.toDto(statsRepository.save(row));

    statsHistoryRepository.save(
        TableStatsHistoryRow.builder()
            .tableUuid(tableUuid)
            .databaseId(request.getDatabaseId())
            .tableName(request.getTableName())
            .stats(request.getStats())
            .recordedAt(now)
            .build());

    return saved;
  }

  @Override
  public Optional<TableStatsDto> getTableStats(String tableUuid) {
    return statsRepository.findById(tableUuid).map(mapper::toDto);
  }

  @Override
  public List<TableStatsDto> listTableStats(String databaseId, String tableName, String tableUuid) {
    return statsRepository.find(databaseId, tableName, tableUuid).stream()
        .map(mapper::toDto)
        .collect(Collectors.toList());
  }

  @Override
  public List<TableStatsHistoryDto> getStatsHistory(String tableUuid, Instant since, int limit) {
    return statsHistoryRepository.find(tableUuid, since, PageRequest.of(0, limit)).stream()
        .map(mapper::toDto)
        .collect(Collectors.toList());
  }

  // --- TableOperationsHistory ---

  @Override
  @Transactional
  public TableOperationsHistoryDto appendHistory(TableOperationsHistoryDto dto) {
    TableOperationsHistoryRow row =
        TableOperationsHistoryRow.builder()
            .id(dto.getId())
            .tableUuid(dto.getTableUuid())
            .databaseName(dto.getDatabaseName())
            .tableName(dto.getTableName())
            .operationType(dto.getOperationType())
            .submittedAt(dto.getSubmittedAt() != null ? dto.getSubmittedAt() : Instant.now())
            .status(dto.getStatus())
            .jobId(dto.getJobId())
            .result(dto.getResult())
            .build();
    return mapper.toDto(historyRepository.save(row));
  }

  @Override
  public List<TableOperationsHistoryDto> getHistory(String tableUuid, int limit) {
    return historyRepository
        .find(null, null, tableUuid, null, null, null, null, PageRequest.of(0, limit)).stream()
        .map(mapper::toDto)
        .collect(Collectors.toList());
  }

  @Override
  public List<TableOperationsHistoryDto> listHistory(
      String databaseName,
      String tableName,
      String tableUuid,
      OperationType operationType,
      OperationHistoryStatus status,
      Instant since,
      Instant until,
      int limit) {
    return historyRepository
        .find(
            databaseName,
            tableName,
            tableUuid,
            operationType,
            status,
            since,
            until,
            PageRequest.of(0, limit))
        .stream()
        .map(mapper::toDto)
        .collect(Collectors.toList());
  }
}
