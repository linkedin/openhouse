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
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Table {

  private String tableUuid;
  private String databaseName;
  private String tableId;

  @Builder.Default private Map<String, String> tableProperties = Collections.emptyMap();

  private TableStats stats;

  /** When the current snapshot was last written. Stamped server-side on every upsert. */
  private Instant updatedAt;
}
