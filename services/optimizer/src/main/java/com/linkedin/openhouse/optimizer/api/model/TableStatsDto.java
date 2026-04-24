package com.linkedin.openhouse.optimizer.api.model;

import java.time.Instant;
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
  private String databaseId;

  /** Denormalized table name for display. */
  private String tableName;

  /** Combined snapshot + delta stats payload, stored as JSON. */
  private TableStats stats;

  /** Current table properties snapshot (e.g. maintenance opt-in flags). */
  private Map<String, String> tableProperties;

  /** When this row was last written. Used for staleness monitoring. */
  private Instant updatedAt;
}
