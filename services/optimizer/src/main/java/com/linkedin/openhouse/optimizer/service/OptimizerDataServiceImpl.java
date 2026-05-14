package com.linkedin.openhouse.optimizer.service;

import com.linkedin.openhouse.optimizer.api.model.CompleteOperationRequest;
import com.linkedin.openhouse.optimizer.api.model.OperationStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsDto;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.api.model.TableStats;
import com.linkedin.openhouse.optimizer.api.model.TableStatsDto;
import com.linkedin.openhouse.optimizer.api.model.TableStatsHistoryDto;
import com.linkedin.openhouse.optimizer.api.model.UpsertTableStatsRequest;
import com.linkedin.openhouse.optimizer.db.TableOperationsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableStatsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.mapper.ApiModelMapper;
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

/** Implementation of {@link OptimizerDataService}. */
@Service
@RequiredArgsConstructor
public class OptimizerDataServiceImpl implements OptimizerDataService {

  private final TableOperationsRepository operationsRepository;
  private final TableOperationsHistoryRepository historyRepository;
  private final TableStatsRepository statsRepository;
  private final TableStatsHistoryRepository statsHistoryRepository;
  private final ApiModelMapper apiMapper;
  private final ModelDbMapper dbMapper;

  // --- TableOperations ---

  @Override
  public List<TableOperationsDto> listTableOperations(
      Optional<OperationType> operationType,
      Optional<OperationStatus> status,
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid) {
    return operationsRepository
        .find(
            operationType
                .map(t -> dbMapper.toDbOperationType(apiMapper.toModelOperationType(t)))
                .orElse(null),
            status
                .map(s -> dbMapper.toDbOperationStatus(apiMapper.toModelOperationStatus(s)))
                .orElse(null),
            tableUuid.orElse(null),
            databaseName.orElse(null),
            tableName.orElse(null))
        .stream()
        .map(dbMapper::toOperation)
        .map(apiMapper::toDto)
        .collect(Collectors.toList());
  }

  @Override
  @Transactional
  public Optional<TableOperationsHistoryDto> completeOperation(CompleteOperationRequest request) {
    return operationsRepository
        .findById(request.getOperationId())
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
                      .status(
                          dbMapper.toDbHistoryStatus(
                              apiMapper.toModelHistoryStatus(request.getStatus())))
                      .build();
              return apiMapper.toDto(dbMapper.toHistory(historyRepository.save(historyRow)));
            });
  }

  @Override
  public Optional<TableOperationsDto> getTableOperation(String id) {
    return operationsRepository.findById(id).map(dbMapper::toOperation).map(apiMapper::toDto);
  }

  // --- TableStats ---

  @Override
  @Transactional
  public TableStatsDto upsertTableStats(String tableUuid, UpsertTableStatsRequest request) {
    Instant now = Instant.now();
    com.linkedin.openhouse.optimizer.model.TableStats modelStats =
        apiMapper.toModelStats(request.getStats());

    TableStatsRow row =
        statsRepository
            .findById(tableUuid)
            .map(
                existing ->
                    existing
                        .toBuilder()
                        .databaseName(request.getDatabaseName())
                        .tableName(request.getTableName())
                        .snapshot(dbMapper.toDbSnapshot(modelStats))
                        .tableProperties(request.getTableProperties())
                        .updatedAt(now)
                        .build())
            .orElse(
                TableStatsRow.builder()
                    .tableUuid(tableUuid)
                    .databaseName(request.getDatabaseName())
                    .tableName(request.getTableName())
                    .snapshot(dbMapper.toDbSnapshot(modelStats))
                    .tableProperties(request.getTableProperties())
                    .updatedAt(now)
                    .build());
    TableStatsRow saved = statsRepository.save(row);

    statsHistoryRepository.save(
        TableStatsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(tableUuid)
            .databaseName(request.getDatabaseName())
            .tableName(request.getTableName())
            .snapshot(dbMapper.toDbSnapshot(modelStats))
            .delta(dbMapper.toDbDelta(modelStats))
            .recordedAt(now)
            .build());

    return toTableStatsDto(saved);
  }

  @Override
  public Optional<TableStatsDto> getTableStats(String tableUuid) {
    return statsRepository.findById(tableUuid).map(this::toTableStatsDto);
  }

  @Override
  public List<TableStatsDto> listTableStats(
      Optional<String> databaseName, Optional<String> tableName, Optional<String> tableUuid) {
    return statsRepository
        .find(databaseName.orElse(null), tableName.orElse(null), tableUuid.orElse(null)).stream()
        .map(this::toTableStatsDto)
        .collect(Collectors.toList());
  }

  @Override
  public List<TableStatsHistoryDto> getStatsHistory(
      String tableUuid, Optional<Instant> since, int limit) {
    return statsHistoryRepository.find(tableUuid, since.orElse(null), PageRequest.of(0, limit))
        .stream()
        .map(this::toTableStatsHistoryDto)
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
            .operationType(
                dbMapper.toDbOperationType(apiMapper.toModelOperationType(dto.getOperationType())))
            .completedAt(dto.getCompletedAt() != null ? dto.getCompletedAt() : Instant.now())
            .status(dbMapper.toDbHistoryStatus(apiMapper.toModelHistoryStatus(dto.getStatus())))
            .build();
    return apiMapper.toDto(dbMapper.toHistory(historyRepository.save(row)));
  }

  @Override
  public List<TableOperationsHistoryDto> getHistory(String tableUuid, int limit) {
    return historyRepository
        .findByTableUuidOrderByCompletedAtDesc(tableUuid, PageRequest.of(0, limit)).stream()
        .map(dbMapper::toHistory)
        .map(apiMapper::toDto)
        .collect(Collectors.toList());
  }

  // --- private helpers ---

  /**
   * Assemble a wire {@link TableStatsDto} from a {@link TableStatsRow}. The current-state row holds
   * only the snapshot — deltas live exclusively on history rows.
   */
  private TableStatsDto toTableStatsDto(TableStatsRow row) {
    com.linkedin.openhouse.optimizer.model.TableStats modelStats =
        dbMapper.joinStats(row.getSnapshot(), null);
    TableStats apiStats = apiMapper.toApiStats(modelStats);
    return TableStatsDto.builder()
        .tableUuid(row.getTableUuid())
        .databaseName(row.getDatabaseName())
        .tableName(row.getTableName())
        .stats(apiStats)
        .tableProperties(row.getTableProperties())
        .updatedAt(row.getUpdatedAt())
        .build();
  }

  /** Assemble a wire {@link TableStatsHistoryDto} from a {@link TableStatsHistoryRow}. */
  private TableStatsHistoryDto toTableStatsHistoryDto(TableStatsHistoryRow row) {
    com.linkedin.openhouse.optimizer.model.TableStats modelStats =
        dbMapper.joinStats(row.getSnapshot(), row.getDelta());
    TableStats apiStats = apiMapper.toApiStats(modelStats);
    return TableStatsHistoryDto.builder()
        .id(row.getId())
        .tableUuid(row.getTableUuid())
        .databaseName(row.getDatabaseName())
        .tableName(row.getTableName())
        .stats(apiStats)
        .recordedAt(row.getRecordedAt())
        .build();
  }
}
