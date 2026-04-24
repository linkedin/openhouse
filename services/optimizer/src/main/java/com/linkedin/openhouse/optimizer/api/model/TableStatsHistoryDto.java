package com.linkedin.openhouse.optimizer.api.model;

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
public class TableStatsHistoryDto {

  /** Auto-increment primary key. */
  private Long id;

  /** Stable Iceberg table UUID. */
  private String tableUuid;

  /** Denormalized database name for display. */
  private String databaseId;

  /** Denormalized table name for display. */
  private String tableName;

  /** Snapshot + delta stats from this commit event. */
  private TableStats stats;

  /** When this history row was recorded. */
  private Instant recordedAt;
}
