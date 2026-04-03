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

  private Long id;
  private String tableUuid;
  private String databaseId;
  private String tableName;
  private TableStats stats;
  private Instant recordedAt;
}
