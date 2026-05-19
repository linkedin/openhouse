package com.linkedin.openhouse.optimizer.api.model;

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
  private TableStatsPayloadDto stats;

  /** Current table properties snapshot (e.g. maintenance opt-in flags). */
  private Map<String, String> tableProperties;

  /** When this row was last written. Used for staleness monitoring. */
  private Instant updatedAt;

  /** Convert to the internal-model counterpart. */
  public com.linkedin.openhouse.optimizer.model.TableStats toModel() {
    com.linkedin.openhouse.optimizer.model.TableStats payload =
        stats == null ? new com.linkedin.openhouse.optimizer.model.TableStats() : stats.toModel();
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
  public static TableStatsDto fromModel(com.linkedin.openhouse.optimizer.model.TableStats m) {
    if (m == null) {
      return null;
    }
    return TableStatsDto.builder()
        .tableUuid(m.getTableUuid())
        .databaseName(m.getDatabaseName())
        .tableName(m.getTableName())
        .stats(
            TableStatsPayloadDto.builder()
                .snapshot(TableStatsPayloadDto.SnapshotMetricsDto.fromModel(m.getSnapshot()))
                .delta(TableStatsPayloadDto.CommitDeltaDto.fromModel(m.getDelta()))
                .build())
        .tableProperties(m.getTableProperties())
        .updatedAt(m.getUpdatedAt())
        .build();
  }
}
