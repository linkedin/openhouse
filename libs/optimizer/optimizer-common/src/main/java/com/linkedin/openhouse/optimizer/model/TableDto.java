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
 * by the analyzer (decides whether to produce a {@link TableOperationDto}) and the scheduler (reads
 * stats for bin-packing).
 *
 * <p>Conversion methods cross into the DB layer one-way; the inverse lives on the api side. db/
 * types know nothing about model/ or api/.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class TableDto {

  /**
   * Canonical table property set by OpenHouse holding the table type ({@code PRIMARY_TABLE} /
   * {@code REPLICA_TABLE}). Mirrors {@code getCanonicalFieldName("tableType")}; duplicated here as
   * a literal so the analyzer module does not depend on the internal catalog. The Tables Service
   * back-fills it to {@code PRIMARY_TABLE}, so it is present on every table.
   */
  public static final String TABLE_TYPE_PROPERTY = "openhouse.tableType";

  /** Value of {@link #TABLE_TYPE_PROPERTY} for a primary (non-replica) table. */
  public static final String PRIMARY_TABLE_TYPE = "PRIMARY_TABLE";

  /** Stable table identity from the Tables Service. Survives renames; rotates on drop+recreate. */
  private String tableUuid;

  /** Database the table lives in. */
  private String databaseName;

  /** Iceberg table identifier (table name, not UUID). */
  private String tableId;

  /** Current table-property map (e.g. maintenance opt-in flags). Never null. */
  @Builder.Default private Map<String, String> tableProperties = Collections.emptyMap();

  /** Latest snapshot stats for this table. Delta is null when read from the current-state row. */
  private TableStatsDto stats;

  /** When the current snapshot was last written. Stamped server-side on every upsert. */
  private Instant updatedAt;

  /**
   * Whether this is a primary table (as opposed to a replica). Derived from the canonical {@code
   * openhouse.tableType} property; an absent or unrecognized value is treated as primary, matching
   * the Tables Service back-fill and the legacy jobs scheduler default — so maintenance is not
   * skipped for a table that happens to lack the property.
   *
   * <p>A replica is maintained by its primary in another cluster, so operations that would change
   * the table's committed Iceberg metadata or snapshots (e.g. snapshot expiration, retention,
   * compaction) must not run when this is false. Operations that don't touch committed metadata —
   * stats collection and orphan-file deletion — may; analyzers declare per-operation safety via
   * {@code OperationAnalyzer.runsOnReplica()}.
   */
  public boolean isPrimary() {
    String tableType = tableProperties.get(TABLE_TYPE_PROPERTY);
    return tableType == null || PRIMARY_TABLE_TYPE.equalsIgnoreCase(tableType);
  }

  /**
   * Project to the current-state DB row. {@code table_stats} carries the snapshot only — per-commit
   * deltas live on {@code table_stats_history} (see {@link TableStatsHistoryDto#toRow()}).
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

  /** Build a {@link TableDto} from a current-state DB row. */
  public static TableDto fromRow(TableStatsRow row) {
    if (row == null) {
      return null;
    }
    return TableDto.builder()
        .tableUuid(row.getTableUuid())
        .databaseName(row.getDatabaseName())
        .tableId(row.getTableName())
        .tableProperties(
            row.getTableProperties() != null ? row.getTableProperties() : Collections.emptyMap())
        // table_stats holds only the snapshot — deltas live on the history table.
        .stats(TableStatsDto.fromRows(row.getSnapshot(), null))
        .updatedAt(row.getUpdatedAt())
        .build();
  }
}
