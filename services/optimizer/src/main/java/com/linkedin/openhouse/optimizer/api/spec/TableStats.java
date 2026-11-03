package com.linkedin.openhouse.optimizer.api.spec;

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
public class TableStats {

  /** Stable Iceberg table UUID. Primary key of the stats row. */
  private String tableUuid;

  /** Denormalized database name for display. */
  private String databaseName;

  /** Denormalized table name for display. */
  private String tableName;

  /** Combined snapshot + delta stats payload, stored as JSON. */
  private TableStatsPayload stats;

  /** Current table properties snapshot (e.g. maintenance opt-in flags). */
  private Map<String, String> tableProperties;

  /** When this row was last written. Used for staleness monitoring. */
  private Instant updatedAt;

  /** Convert to the internal-model counterpart. */
  public com.linkedin.openhouse.optimizer.model.TableStatsDto toModel() {
    com.linkedin.openhouse.optimizer.model.TableStatsDto payload =
        stats == null
            ? new com.linkedin.openhouse.optimizer.model.TableStatsDto()
            : stats.toModel();
    return payload
        .toBuilder()
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .tableProperties(tableProperties != null ? tableProperties : Collections.emptyMap())
        .updatedAt(updatedAt)
        .build();
  }

  /** Build a wire DTO from the internal-model counterpart. */
  public static TableStats fromModel(com.linkedin.openhouse.optimizer.model.TableStatsDto m) {
    if (m == null) {
      return null;
    }
    return TableStats.builder()
        .tableUuid(m.getTableUuid())
        .databaseName(m.getDatabaseName())
        .tableName(m.getTableName())
        .stats(
            TableStatsPayload.builder()
                .snapshot(TableStatsPayload.SnapshotMetricsDto.fromModel(m.getSnapshot()))
                .delta(TableStatsPayload.CommitDeltaDto.fromModel(m.getDelta()))
                .build())
        .tableProperties(m.getTableProperties())
        .updatedAt(m.getUpdatedAt())
        .build();
  }
}
