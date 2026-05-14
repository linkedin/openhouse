package com.linkedin.openhouse.optimizer.api.model;

import com.linkedin.openhouse.optimizer.model.TableStats;
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

  /** UUID primary key set by the caller. */
  private String id;

  /** Stable Iceberg table UUID. */
  private String tableUuid;

  /** Denormalized database name for display. */
  private String databaseName;

  /** Denormalized table name for display. */
  private String tableName;

  /** Snapshot + delta stats from this commit event. */
  private TableStats stats;

  /** When this history row was recorded. */
  private Instant recordedAt;
}
