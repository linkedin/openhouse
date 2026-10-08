package com.linkedin.openhouse.optimizer.db;

import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * JPA entity representing a per-table stats snapshot in the optimizer DB.
 *
 * <p>Written by the Tables Service on every Iceberg commit. Read by the Analyzer directly via JPA
 * to enumerate tables and check scheduling eligibility. Holds only point-in-time snapshot data;
 * per-commit deltas live exclusively on {@link TableStatsHistoryRow}.
 */
@Entity
@Table(
    name = "table_stats",
    indexes = {@Index(name = "idx_ts_db_table", columnList = "database_name, table_name")})
@Getter
@EqualsAndHashCode
@Builder(toBuilder = true)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class TableStatsRow {

  /** Stable Iceberg table UUID. Primary key. */
  @Id
  @Column(name = "table_uuid", nullable = false, length = 36)
  private String tableUuid;

  /** Denormalized database name. */
  @Column(name = "database_name", nullable = false, length = 128)
  private String databaseName;

  /** Denormalized table name. */
  @Column(name = "table_name", nullable = false, length = 128)
  private String tableName;

  /** Latest snapshot fields. Stored as JSON text for compatibility with the existing schema. */
  @Convert(converter = JsonColumns.SnapshotMetricsConverter.class)
  @Column(name = "snapshot", columnDefinition = "TEXT")
  private SnapshotMetrics snapshot;

  /** Current table-property map (e.g. maintenance opt-in flags). Stored as JSON text. */
  @Convert(converter = JsonColumns.StringMapConverter.class)
  @Column(name = "table_properties", columnDefinition = "TEXT")
  private Map<String, String> tableProperties;

  /** Set on every upsert. Used for stats pipeline staleness monitoring. */
  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;

  /** Convert this current-state row to the Spring-free stats model. */
  public TableStatsDto toModel() {
    return TableStatsDto.builder()
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .tableProperties(tableProperties == null ? Collections.emptyMap() : tableProperties)
        .snapshot(snapshot == null ? null : snapshot.toModel())
        .updatedAt(updatedAt)
        .build();
  }

  /** Convert this current-state row to the table view consumed by analyzers. */
  public TableDto toTableModel() {
    return TableDto.builder()
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableId(tableName)
        .tableProperties(tableProperties == null ? Collections.emptyMap() : tableProperties)
        .stats(
            snapshot == null ? null : TableStatsDto.builder().snapshot(snapshot.toModel()).build())
        .updatedAt(updatedAt)
        .build();
  }

  /** Build a current-state persistence row from the Spring-free stats model. */
  public static TableStatsRow fromModel(TableStatsDto stats) {
    if (stats == null) {
      return null;
    }
    return TableStatsRow.builder()
        .tableUuid(stats.getTableUuid())
        .databaseName(stats.getDatabaseName())
        .tableName(stats.getTableName())
        .snapshot(SnapshotMetrics.fromModel(stats.getSnapshot()))
        .tableProperties(
            stats.getTableProperties() == null
                ? Collections.emptyMap()
                : stats.getTableProperties())
        .updatedAt(stats.getUpdatedAt())
        .build();
  }
}
