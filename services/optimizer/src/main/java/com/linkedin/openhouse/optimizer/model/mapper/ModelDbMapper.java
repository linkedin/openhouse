package com.linkedin.openhouse.optimizer.model.mapper;

import com.linkedin.openhouse.optimizer.db.TableOperationsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.HistoryStatus;
import com.linkedin.openhouse.optimizer.model.OperationStatus;
import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.Table;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistory;
import com.linkedin.openhouse.optimizer.model.TableStats;
import java.util.Collections;
import org.springframework.stereotype.Component;

/**
 * Converts between internal {@code model/} domain objects and database row entities.
 *
 * <p>The only place inside {@code model/} where {@code db/} types are referenced — this is the
 * boundary at which the internal model meets the database layer. Pure data types under {@code
 * model/} stay free of any DB-side imports.
 *
 * <p>Each layer carries its own per-layer enum + payload types. This mapper translates between
 * model/-side and db/-side counterparts by name.
 */
@Component
public class ModelDbMapper {

  // --- TableOperationsRow <-> TableOperation ---

  public TableOperation toOperation(TableOperationsRow row) {
    if (row == null) {
      return null;
    }
    return TableOperation.builder()
        .id(row.getId())
        .tableUuid(row.getTableUuid())
        .databaseName(row.getDatabaseName())
        .tableName(row.getTableName())
        .operationType(toModelOperationType(row.getOperationType()))
        .status(toModelOperationStatus(row.getStatus()))
        .createdAt(row.getCreatedAt())
        .scheduledAt(row.getScheduledAt())
        .build();
  }

  public TableOperationsRow toRow(TableOperation op) {
    if (op == null) {
      return null;
    }
    return TableOperationsRow.builder()
        .id(op.getId())
        .tableUuid(op.getTableUuid())
        .databaseName(op.getDatabaseName())
        .tableName(op.getTableName())
        .operationType(toDbOperationType(op.getOperationType()))
        .status(toDbOperationStatus(op.getStatus()))
        .createdAt(op.getCreatedAt())
        .scheduledAt(op.getScheduledAt())
        .version(0L)
        .build();
  }

  // --- TableOperationsHistoryRow <-> TableOperationsHistory ---

  public TableOperationsHistory toHistory(TableOperationsHistoryRow row) {
    if (row == null) {
      return null;
    }
    return TableOperationsHistory.builder()
        .id(row.getId())
        .tableUuid(row.getTableUuid())
        .databaseName(row.getDatabaseName())
        .tableName(row.getTableName())
        .operationType(toModelOperationType(row.getOperationType()))
        .completedAt(row.getCompletedAt())
        .status(toModelHistoryStatus(row.getStatus()))
        .build();
  }

  public TableOperationsHistoryRow toRow(TableOperationsHistory history) {
    if (history == null) {
      return null;
    }
    return TableOperationsHistoryRow.builder()
        .id(history.getId())
        .tableUuid(history.getTableUuid())
        .databaseName(history.getDatabaseName())
        .tableName(history.getTableName())
        .operationType(toDbOperationType(history.getOperationType()))
        .completedAt(history.getCompletedAt())
        .status(toDbHistoryStatus(history.getStatus()))
        .build();
  }

  // --- TableStatsRow -> Table ---

  public Table toTable(TableStatsRow row) {
    if (row == null) {
      return null;
    }
    return Table.builder()
        .tableUuid(row.getTableUuid())
        .databaseName(row.getDatabaseName())
        .tableId(row.getTableName())
        .tableProperties(
            row.getTableProperties() != null ? row.getTableProperties() : Collections.emptyMap())
        .stats(toModelStats(row.getStats()))
        .build();
  }

  // --- TableStats payload ---

  public TableStats toModelStats(com.linkedin.openhouse.optimizer.db.TableStats dbStats) {
    if (dbStats == null) {
      return null;
    }
    return TableStats.builder()
        .snapshot(toModelSnapshot(dbStats.getSnapshot()))
        .delta(toModelDelta(dbStats.getDelta()))
        .build();
  }

  public com.linkedin.openhouse.optimizer.db.TableStats toDbStats(TableStats modelStats) {
    if (modelStats == null) {
      return null;
    }
    return com.linkedin.openhouse.optimizer.db.TableStats.builder()
        .snapshot(toDbSnapshot(modelStats.getSnapshot()))
        .delta(toDbDelta(modelStats.getDelta()))
        .build();
  }

  public TableStatsHistoryRow toStatsHistoryRow(
      String id,
      String tableUuid,
      String databaseName,
      String tableName,
      TableStats stats,
      java.time.Instant recordedAt) {
    return TableStatsHistoryRow.builder()
        .id(id)
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .stats(toDbStats(stats))
        .recordedAt(recordedAt)
        .build();
  }

  // --- enum helpers ---

  public OperationType toModelOperationType(com.linkedin.openhouse.optimizer.db.OperationType v) {
    return v == null ? null : OperationType.valueOf(v.name());
  }

  public com.linkedin.openhouse.optimizer.db.OperationType toDbOperationType(OperationType v) {
    return v == null ? null : com.linkedin.openhouse.optimizer.db.OperationType.valueOf(v.name());
  }

  public OperationStatus toModelOperationStatus(
      com.linkedin.openhouse.optimizer.db.OperationStatus v) {
    return v == null ? null : OperationStatus.valueOf(v.name());
  }

  public com.linkedin.openhouse.optimizer.db.OperationStatus toDbOperationStatus(
      OperationStatus v) {
    return v == null ? null : com.linkedin.openhouse.optimizer.db.OperationStatus.valueOf(v.name());
  }

  public HistoryStatus toModelHistoryStatus(com.linkedin.openhouse.optimizer.db.HistoryStatus v) {
    return v == null ? null : HistoryStatus.valueOf(v.name());
  }

  public com.linkedin.openhouse.optimizer.db.HistoryStatus toDbHistoryStatus(HistoryStatus v) {
    return v == null ? null : com.linkedin.openhouse.optimizer.db.HistoryStatus.valueOf(v.name());
  }

  // --- TableStats inner classes ---

  private TableStats.SnapshotMetrics toModelSnapshot(
      com.linkedin.openhouse.optimizer.db.TableStats.SnapshotMetrics v) {
    if (v == null) {
      return null;
    }
    return TableStats.SnapshotMetrics.builder()
        .clusterId(v.getClusterId())
        .tableVersion(v.getTableVersion())
        .tableLocation(v.getTableLocation())
        .tableSizeBytes(v.getTableSizeBytes())
        .numCurrentFiles(v.getNumCurrentFiles())
        .build();
  }

  private com.linkedin.openhouse.optimizer.db.TableStats.SnapshotMetrics toDbSnapshot(
      TableStats.SnapshotMetrics v) {
    if (v == null) {
      return null;
    }
    return com.linkedin.openhouse.optimizer.db.TableStats.SnapshotMetrics.builder()
        .clusterId(v.getClusterId())
        .tableVersion(v.getTableVersion())
        .tableLocation(v.getTableLocation())
        .tableSizeBytes(v.getTableSizeBytes())
        .numCurrentFiles(v.getNumCurrentFiles())
        .build();
  }

  private TableStats.CommitDelta toModelDelta(
      com.linkedin.openhouse.optimizer.db.TableStats.CommitDelta v) {
    if (v == null) {
      return null;
    }
    return TableStats.CommitDelta.builder()
        .numFilesAdded(v.getNumFilesAdded())
        .numFilesDeleted(v.getNumFilesDeleted())
        .addedSizeBytes(v.getAddedSizeBytes())
        .deletedSizeBytes(v.getDeletedSizeBytes())
        .build();
  }

  private com.linkedin.openhouse.optimizer.db.TableStats.CommitDelta toDbDelta(
      TableStats.CommitDelta v) {
    if (v == null) {
      return null;
    }
    return com.linkedin.openhouse.optimizer.db.TableStats.CommitDelta.builder()
        .numFilesAdded(v.getNumFilesAdded())
        .numFilesDeleted(v.getNumFilesDeleted())
        .addedSizeBytes(v.getAddedSizeBytes())
        .deletedSizeBytes(v.getDeletedSizeBytes())
        .build();
  }
}
