package com.linkedin.openhouse.analyzer.model;

import com.linkedin.openhouse.optimizer.entity.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.TableStats;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * An OpenHouse table enriched with stats and properties, built by combining data sources. This is
 * the input to the analysis pipeline: analyzers evaluate a {@code Table} and decide whether to
 * produce a {@link TableOperation}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Table {

  private String tableUuid;
  private String databaseId;
  private String tableId;

  @Builder.Default private Map<String, String> tableProperties = Collections.emptyMap();

  private TableStats stats;

  /** Build a {@code Table} from a {@code table_stats} row. */
  public static Table from(TableStatsRow row) {
    return Table.builder()
        .tableUuid(row.getTableUuid())
        .databaseId(row.getDatabaseId())
        .tableId(row.getTableName())
        .tableProperties(
            row.getTableProperties() != null ? row.getTableProperties() : Collections.emptyMap())
        .stats(row.getStats())
        .build();
  }
}
