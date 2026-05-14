package com.linkedin.openhouse.optimizer.model.mapper;

import com.linkedin.openhouse.optimizer.api.model.TableOperationsDto;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.model.HistoryStatus;
import com.linkedin.openhouse.optimizer.model.OperationStatus;
import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistory;
import com.linkedin.openhouse.optimizer.model.TableStats;
import org.springframework.stereotype.Component;

/**
 * Converts between wire-API DTOs and internal {@code model/} domain objects.
 *
 * <p>The only place inside {@code model/} where {@code api/} types are referenced — this is the
 * boundary at which the internal model meets the wire-API. Pure data types under {@code model/}
 * stay free of any api-side imports.
 *
 * <p>API-layer enums + payloads are intentionally separate Java types from the internal-model
 * counterparts; the two sides evolve independently. This mapper translates by name.
 */
@Component
public class ApiModelMapper {

  // --- TableOperationsDto <-> TableOperation ---

  public TableOperation toOperation(TableOperationsDto dto) {
    if (dto == null) {
      return null;
    }
    return TableOperation.builder()
        .id(dto.getId())
        .tableUuid(dto.getTableUuid())
        .databaseName(dto.getDatabaseName())
        .tableName(dto.getTableName())
        .operationType(toModelOperationType(dto.getOperationType()))
        .status(toModelOperationStatus(dto.getStatus()))
        .createdAt(dto.getCreatedAt())
        .scheduledAt(dto.getScheduledAt())
        .build();
  }

  public TableOperationsDto toDto(TableOperation op) {
    if (op == null) {
      return null;
    }
    return TableOperationsDto.builder()
        .id(op.getId())
        .tableUuid(op.getTableUuid())
        .databaseName(op.getDatabaseName())
        .tableName(op.getTableName())
        .operationType(toApiOperationType(op.getOperationType()))
        .status(toApiOperationStatus(op.getStatus()))
        .createdAt(op.getCreatedAt())
        .scheduledAt(op.getScheduledAt())
        .build();
  }

  // --- TableOperationsHistoryDto <-> TableOperationsHistory ---

  public TableOperationsHistory toHistory(TableOperationsHistoryDto dto) {
    if (dto == null) {
      return null;
    }
    return TableOperationsHistory.builder()
        .id(dto.getId())
        .tableUuid(dto.getTableUuid())
        .databaseName(dto.getDatabaseName())
        .tableName(dto.getTableName())
        .operationType(toModelOperationType(dto.getOperationType()))
        .completedAt(dto.getCompletedAt())
        .status(toModelHistoryStatus(dto.getStatus()))
        .build();
  }

  public TableOperationsHistoryDto toDto(TableOperationsHistory history) {
    if (history == null) {
      return null;
    }
    return TableOperationsHistoryDto.builder()
        .id(history.getId())
        .tableUuid(history.getTableUuid())
        .databaseName(history.getDatabaseName())
        .tableName(history.getTableName())
        .operationType(toApiOperationType(history.getOperationType()))
        .completedAt(history.getCompletedAt())
        .status(toApiHistoryStatus(history.getStatus()))
        .build();
  }

  // --- TableStats payload ---

  public TableStats toModelStats(com.linkedin.openhouse.optimizer.api.model.TableStats apiStats) {
    if (apiStats == null) {
      return null;
    }
    return TableStats.builder()
        .snapshot(toModelSnapshot(apiStats.getSnapshot()))
        .delta(toModelDelta(apiStats.getDelta()))
        .build();
  }

  public com.linkedin.openhouse.optimizer.api.model.TableStats toApiStats(TableStats modelStats) {
    if (modelStats == null) {
      return null;
    }
    return com.linkedin.openhouse.optimizer.api.model.TableStats.builder()
        .snapshot(toApiSnapshot(modelStats.getSnapshot()))
        .delta(toApiDelta(modelStats.getDelta()))
        .build();
  }

  // --- enum helpers ---

  public OperationType toModelOperationType(
      com.linkedin.openhouse.optimizer.api.model.OperationType apiValue) {
    return apiValue == null ? null : OperationType.valueOf(apiValue.name());
  }

  public com.linkedin.openhouse.optimizer.api.model.OperationType toApiOperationType(
      OperationType modelValue) {
    return modelValue == null
        ? null
        : com.linkedin.openhouse.optimizer.api.model.OperationType.valueOf(modelValue.name());
  }

  public OperationStatus toModelOperationStatus(
      com.linkedin.openhouse.optimizer.api.model.OperationStatus apiValue) {
    return apiValue == null ? null : OperationStatus.valueOf(apiValue.name());
  }

  public com.linkedin.openhouse.optimizer.api.model.OperationStatus toApiOperationStatus(
      OperationStatus modelValue) {
    return modelValue == null
        ? null
        : com.linkedin.openhouse.optimizer.api.model.OperationStatus.valueOf(modelValue.name());
  }

  public HistoryStatus toModelHistoryStatus(
      com.linkedin.openhouse.optimizer.api.model.HistoryStatus apiValue) {
    return apiValue == null ? null : HistoryStatus.valueOf(apiValue.name());
  }

  public com.linkedin.openhouse.optimizer.api.model.HistoryStatus toApiHistoryStatus(
      HistoryStatus modelValue) {
    return modelValue == null
        ? null
        : com.linkedin.openhouse.optimizer.api.model.HistoryStatus.valueOf(modelValue.name());
  }

  // --- TableStats inner classes ---

  private TableStats.SnapshotMetrics toModelSnapshot(
      com.linkedin.openhouse.optimizer.api.model.TableStats.SnapshotMetrics apiValue) {
    if (apiValue == null) {
      return null;
    }
    return TableStats.SnapshotMetrics.builder()
        .clusterId(apiValue.getClusterId())
        .tableVersion(apiValue.getTableVersion())
        .tableLocation(apiValue.getTableLocation())
        .tableSizeBytes(apiValue.getTableSizeBytes())
        .numCurrentFiles(apiValue.getNumCurrentFiles())
        .build();
  }

  private com.linkedin.openhouse.optimizer.api.model.TableStats.SnapshotMetrics toApiSnapshot(
      TableStats.SnapshotMetrics modelValue) {
    if (modelValue == null) {
      return null;
    }
    return com.linkedin.openhouse.optimizer.api.model.TableStats.SnapshotMetrics.builder()
        .clusterId(modelValue.getClusterId())
        .tableVersion(modelValue.getTableVersion())
        .tableLocation(modelValue.getTableLocation())
        .tableSizeBytes(modelValue.getTableSizeBytes())
        .numCurrentFiles(modelValue.getNumCurrentFiles())
        .build();
  }

  private TableStats.CommitDelta toModelDelta(
      com.linkedin.openhouse.optimizer.api.model.TableStats.CommitDelta apiValue) {
    if (apiValue == null) {
      return null;
    }
    return TableStats.CommitDelta.builder()
        .numFilesAdded(apiValue.getNumFilesAdded())
        .numFilesDeleted(apiValue.getNumFilesDeleted())
        .addedSizeBytes(apiValue.getAddedSizeBytes())
        .deletedSizeBytes(apiValue.getDeletedSizeBytes())
        .build();
  }

  private com.linkedin.openhouse.optimizer.api.model.TableStats.CommitDelta toApiDelta(
      TableStats.CommitDelta modelValue) {
    if (modelValue == null) {
      return null;
    }
    return com.linkedin.openhouse.optimizer.api.model.TableStats.CommitDelta.builder()
        .numFilesAdded(modelValue.getNumFilesAdded())
        .numFilesDeleted(modelValue.getNumFilesDeleted())
        .addedSizeBytes(modelValue.getAddedSizeBytes())
        .deletedSizeBytes(modelValue.getDeletedSizeBytes())
        .build();
  }
}
