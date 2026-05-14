package com.linkedin.openhouse.optimizer.api.model;

import com.linkedin.openhouse.optimizer.model.Table;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** DTO for {@code table_stats} — used for response payloads. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableStatsDto {

  /** Stable Iceberg table UUID. Primary key of the stats row. */
  private String tableUuid;

  /** Denormalized database name for display. */
  private String databaseName;

  /** Denormalized table name for display. */
  private String tableName;

  /** Combined snapshot + delta stats payload, stored as JSON. */
  private TableStats stats;

  /** Current table properties snapshot (e.g. maintenance opt-in flags). */
  private Map<String, String> tableProperties;

  /** When this row was last written. Used for staleness monitoring. */
  private Instant updatedAt;

  /** Convert to the internal-model counterpart. */
  public Table toModel() {
    return Table.builder()
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableId(tableName)
        .tableProperties(tableProperties != null ? tableProperties : Collections.emptyMap())
        .stats(stats == null ? null : stats.toModel())
        .updatedAt(updatedAt)
        .build();
  }

  /** Build a wire DTO from the internal-model counterpart. */
  public static TableStatsDto fromModel(Table t) {
    if (t == null) {
      return null;
    }
    return TableStatsDto.builder()
        .tableUuid(t.getTableUuid())
        .databaseName(t.getDatabaseName())
        .tableName(t.getTableId())
        .stats(TableStats.fromModel(t.getStats()))
        .tableProperties(t.getTableProperties())
        .updatedAt(t.getUpdatedAt())
        .build();
  }
}
