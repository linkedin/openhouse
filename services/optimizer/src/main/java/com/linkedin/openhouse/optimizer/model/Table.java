package com.linkedin.openhouse.optimizer.model;

import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * An OpenHouse table enriched with stats and properties, built by combining data sources. Consumed
 * by the analyzer (decides whether to produce a {@link TableOperation}) and the scheduler (reads
 * stats for bin-packing).
 *
 * <p>Conversion methods cross into the DB layer one-way; the inverse lives on the api side. db/
 * types know nothing about model/ or api/.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class Table {

  /** Stable table identity from the Tables Service. Survives renames; rotates on drop+recreate. */
  private String tableUuid;

  /** Database the table lives in. */
  private String databaseName;

  /** Iceberg table identifier (table name, not UUID). */
  private String tableId;

  /** Current table-property map (e.g. maintenance opt-in flags). Never null. */
  @Builder.Default private Map<String, String> tableProperties = Collections.emptyMap();

  /** Latest snapshot stats for this table. Delta is null when read from the current-state row. */
  private TableStats stats;

  /** When the current snapshot was last written. Stamped server-side on every upsert. */
  private Instant updatedAt;

  /**
   * Project to the current-state DB row. {@code table_stats} carries the snapshot only — per-commit
   * deltas live on {@code table_stats_history} (see {@link TableStatsHistory#toRow()}).
   */
  public TableStatsRow toRow() {
    return TableStatsRow.builder()
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableId)
        .snapshot(stats == null ? null : stats.toSnapshotRow())
        .tableProperties(tableProperties)
        .updatedAt(updatedAt)
        .build();
  }

  /** Build a {@link Table} from a current-state DB row. */
  public static Table fromRow(TableStatsRow row) {
    if (row == null) {
      return null;
    }
    return Table.builder()
        .tableUuid(row.getTableUuid())
        .databaseName(row.getDatabaseName())
        .tableId(row.getTableName())
        .tableProperties(
            row.getTableProperties() != null ? row.getTableProperties() : Collections.emptyMap())
        // table_stats holds only the snapshot — deltas live on the history table.
        .stats(TableStats.fromRows(row.getSnapshot(), null))
        .updatedAt(row.getUpdatedAt())
        .build();
  }
}
