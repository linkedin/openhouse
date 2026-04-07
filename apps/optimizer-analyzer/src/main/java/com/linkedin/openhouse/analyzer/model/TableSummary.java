package com.linkedin.openhouse.analyzer.model;

import com.linkedin.openhouse.optimizer.model.TableStats;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Internal representation of a table, decoupled from any external API response model. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableSummary {

  private String tableUuid;
  private String databaseId;
  private String tableId;

  @Builder.Default private Map<String, String> tableProperties = Collections.emptyMap();

  /** Commit stats from the optimizer {@code table_stats} table. Null if no stats recorded yet. */
  private TableStats stats;
}
