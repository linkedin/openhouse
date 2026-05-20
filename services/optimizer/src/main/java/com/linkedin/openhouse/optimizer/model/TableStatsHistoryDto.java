package com.linkedin.openhouse.optimizer.model;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Internal-model view of an append-only per-commit stats history record.
 *
 * <p>One per Iceberg commit. {@link #stats} carries both the snapshot at commit time and the commit
 * delta — consumers can reconstruct change rates over arbitrary time windows.
 *
 * <p>Pure internal-model type — no references to wire-API or DB types.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableStatsHistoryDto {

  /** UUID primary key — set by the caller, not generated server-side. */
  private String id;

  /** Stable table identity from the Tables Service. */
  private String tableUuid;

  /** Denormalized database name for display. */
  private String databaseName;

  /** Denormalized table name for display. */
  private String tableName;

  /** Snapshot + delta for this commit event. */
  private TableStatsDto stats;

  /** When this history row was recorded. */
  private Instant recordedAt;
}
