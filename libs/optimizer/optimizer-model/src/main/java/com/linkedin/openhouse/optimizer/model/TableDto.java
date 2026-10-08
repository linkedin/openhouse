package com.linkedin.openhouse.optimizer.model;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * An OpenHouse table enriched with stats and properties, built by combining data sources. Consumed
 * by the analyzer (decides whether to produce a {@link TableOperationDto}) and the scheduler (reads
 * stats for bin-packing).
 *
 * <p>Knows nothing about the db or api layers; they convert to and from it.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class TableDto {

  /** Stable table identity from the Tables Service. Survives renames; rotates on drop+recreate. */
  private String tableUuid;

  /** Database the table lives in. */
  private String databaseName;

  /** Iceberg table identifier (table name, not UUID). */
  private String tableId;

  /** Current table-property map (e.g. maintenance opt-in flags). Never null. */
  @Builder.Default private Map<String, String> tableProperties = Collections.emptyMap();

  /** Latest snapshot stats for this table. Delta is null when read from the current-state row. */
  private TableStatsDto stats;

  /** When the current snapshot was last written. Stamped server-side on every upsert. */
  private Instant updatedAt;
}
