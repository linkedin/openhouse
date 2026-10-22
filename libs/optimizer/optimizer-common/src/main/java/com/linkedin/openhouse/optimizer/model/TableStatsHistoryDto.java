package com.linkedin.openhouse.optimizer.model;

import com.linkedin.openhouse.optimizer.db.TableStatsHistoryRow;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Internal-model view of an append-only per-commit stats history record.
 *
 * <p>One per Iceberg commit. {@link #stats} carries both the snapshot at commit time and the commit
 * delta — consumers can reconstruct change rates over arbitrary time windows.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableStatsHistoryDto {

  /** UUID primary key — set by the caller, not generated server-side. */
  private String id;

  /** Stable table identity from the Tables Service. */
  private String tableUuid;

  /** Denormalized database name for display. */
  private String databaseName;

  /** Denormalized table name for display. */
  private String tableName;

  /** Snapshot + delta for this commit event. */
  private TableStatsDto stats;

  /** When this history row was recorded. */
  private Instant recordedAt;

  /** Convert to the corresponding DB row. */
  public TableStatsHistoryRow toRow() {
    return TableStatsHistoryRow.builder()
        .id(id)
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .snapshot(stats == null ? null : stats.toSnapshotRow())
        .delta(stats == null ? null : stats.toDeltaRow())
        .recordedAt(recordedAt)
        .build();
  }

  /** Build a {@link TableStatsHistoryDto} from a DB row. */
  public static TableStatsHistoryDto fromRow(TableStatsHistoryRow row) {
    if (row == null) {
      return null;
    }
    return TableStatsHistoryDto.builder()
        .id(row.getId())
        .tableUuid(row.getTableUuid())
        .databaseName(row.getDatabaseName())
        .tableName(row.getTableName())
        .stats(TableStatsDto.fromRows(row.getSnapshot(), row.getDelta()))
        .recordedAt(row.getRecordedAt())
        .build();
  }
}
