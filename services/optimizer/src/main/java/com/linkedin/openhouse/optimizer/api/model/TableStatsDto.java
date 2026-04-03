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

  private String tableUuid;
  private String databaseId;
  private String tableName;
  private TableStats stats;
  private Map<String, String> tableProperties;
  private Instant updatedAt;
}
