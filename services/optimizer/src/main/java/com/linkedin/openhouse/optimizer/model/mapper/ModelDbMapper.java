package com.linkedin.openhouse.optimizer.model.mapper;

import com.linkedin.openhouse.optimizer.db.CommitDeltaMetrics;
import com.linkedin.openhouse.optimizer.db.SnapshotMetrics;
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
 * <p>Each layer carries its own per-layer enum + payload types. The DB layer flattens the wire-side
 * {@code TableStats} envelope into two separate columns ({@code snapshot} and {@code delta}); this
 * mapper joins / splits them at the boundary.
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
        .stats(joinStats(row.getSnapshot(), row.getDelta()))
        .build();
  }

  // --- TableStats payload <-> (snapshot, delta) ---

  /** Join the two DB-side columns into a single internal-model {@link TableStats}. */
  public TableStats joinStats(SnapshotMetrics dbSnapshot, CommitDeltaMetrics dbDelta) {
    if (dbSnapshot == null && dbDelta == null) {
      return null;
    }
    return TableStats.builder()
        .snapshot(toModelSnapshot(dbSnapshot))
        .delta(toModelDelta(dbDelta))
        .build();
  }

  /** Project the internal-model {@link TableStats#getSnapshot()} side. */
  public SnapshotMetrics toDbSnapshot(TableStats modelStats) {
    return modelStats == null ? null : toDbSnapshot(modelStats.getSnapshot());
  }

  /** Project the internal-model {@link TableStats#getDelta()} side. */
  public CommitDeltaMetrics toDbDelta(TableStats modelStats) {
    return modelStats == null ? null : toDbDelta(modelStats.getDelta());
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
        .snapshot(toDbSnapshot(stats))
        .delta(toDbDelta(stats))
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

  // --- inner-payload field copies ---

  private TableStats.SnapshotMetrics toModelSnapshot(SnapshotMetrics v) {
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

  private SnapshotMetrics toDbSnapshot(TableStats.SnapshotMetrics v) {
    if (v == null) {
      return null;
    }
    return SnapshotMetrics.builder()
        .clusterId(v.getClusterId())
        .tableVersion(v.getTableVersion())
        .tableLocation(v.getTableLocation())
        .tableSizeBytes(v.getTableSizeBytes())
        .numCurrentFiles(v.getNumCurrentFiles())
        .build();
  }

  private TableStats.CommitDelta toModelDelta(CommitDeltaMetrics v) {
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

  private CommitDeltaMetrics toDbDelta(TableStats.CommitDelta v) {
    if (v == null) {
      return null;
    }
    return CommitDeltaMetrics.builder()
        .numFilesAdded(v.getNumFilesAdded())
        .numFilesDeleted(v.getNumFilesDeleted())
        .addedSizeBytes(v.getAddedSizeBytes())
        .deletedSizeBytes(v.getDeletedSizeBytes())
        .build();
  }
}
