package com.linkedin.openhouse.optimizer.api.spec;

import com.linkedin.openhouse.optimizer.model.TableStatsHistoryDto;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** DTO for {@code table_stats_history} — used for response payloads. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableStatsHistory {

  /** UUID primary key set by the caller. */
  private String id;

  /** Stable Iceberg table UUID. */
  private String tableUuid;

  /** Denormalized database name for display. */
  private String databaseName;

  /** Denormalized table name for display. */
  private String tableName;

  /** Snapshot + delta stats from this commit event. */
  private TableStatsPayload stats;

  /** When this history row was recorded. */
  private Instant recordedAt;

  /** Convert to the internal-model counterpart. */
  public TableStatsHistoryDto toModel() {
    return TableStatsHistoryDto.builder()
        .id(id)
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .stats(stats == null ? null : stats.toModel())
        .recordedAt(recordedAt)
        .build();
  }

  /** Build a wire DTO from the internal-model counterpart. */
  public static TableStatsHistory fromModel(TableStatsHistoryDto h) {
    if (h == null) {
      return null;
    }
    return TableStatsHistory.builder()
        .id(h.getId())
        .tableUuid(h.getTableUuid())
        .databaseName(h.getDatabaseName())
        .tableName(h.getTableName())
        .stats(TableStatsPayload.fromModel(h.getStats()))
        .recordedAt(h.getRecordedAt())
        .build();
  }
}
