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
 * by the analyzer (decides whether to produce a {@link TableOperation}) and the scheduler (reads
 * stats for bin-packing).
 *
 * <p>Pure internal-model type — no references to wire-API or DB types. Construct via {@link
 * com.linkedin.openhouse.optimizer.model.mapper.ModelDbMapper#toTable} at the DB boundary.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class Table {

  /** Stable table identity from the Tables Service. Survives renames; rotates on drop+recreate. */
  private String tableUuid;

  /** Database the table lives in. */
  private String databaseName;

  /** Iceberg table identifier (table name, not UUID). */
  private String tableId;

  /** Current table-property map (e.g. maintenance opt-in flags). Never null. */
  @Builder.Default private Map<String, String> tableProperties = Collections.emptyMap();

  /** Latest snapshot stats for this table. Delta is null when read from the current-state row. */
  private TableStats stats;

  /** When the current snapshot was last written. Stamped server-side on every upsert. */
  private Instant updatedAt;
}
