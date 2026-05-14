package com.linkedin.openhouse.optimizer.model;

import com.linkedin.openhouse.optimizer.entity.TableStatsRow;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * An OpenHouse table enriched with stats and properties, built by combining data sources. Consumed
 * by the analyzer (decides whether to produce a {@link TableOperation}) and the scheduler (reads
 * stats for bin-packing).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Table {

  private String tableUuid;
  private String databaseName;
  private String tableId;

  @Builder.Default private Map<String, String> tableProperties = Collections.emptyMap();

  private TableStats stats;

  /** Build a {@code Table} from a {@code table_stats} row. */
  public static Table from(TableStatsRow row) {
    return Table.builder()
        .tableUuid(row.getTableUuid())
        .databaseName(row.getDatabaseName())
        .tableId(row.getTableName())
        .tableProperties(
            row.getTableProperties() != null ? row.getTableProperties() : Collections.emptyMap())
        .stats(row.getStats())
        .build();
  }
}
