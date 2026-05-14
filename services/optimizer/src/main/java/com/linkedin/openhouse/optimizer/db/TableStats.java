package com.linkedin.openhouse.optimizer.db;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DB-layer stats payload — stored as a JSON blob in the {@code stats} column of {@code table_stats}
 * and {@code table_stats_history}.
 *
 * <p>Self-contained: no references to api/ or model/ types.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableStats {

  /** Snapshot fields — overwritten on every upsert. */
  private SnapshotMetrics snapshot;

  /** Delta fields — accumulated across commit events. */
  private CommitDelta delta;

  /** Point-in-time metadata read from Iceberg at scan time. */
  @Data
  @Builder(toBuilder = true)
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SnapshotMetrics {
    private String clusterId;
    private String tableVersion;
    private String tableLocation;
    private Long tableSizeBytes;
    /** Total number of data files as of the latest snapshot. */
    private Long numCurrentFiles;
  }

  /** Per-commit incremental counters; accumulated across all recorded commit events. */
  @Data
  @Builder(toBuilder = true)
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class CommitDelta {
    private Long numFilesAdded;
    private Long numFilesDeleted;
    private Long addedSizeBytes;
    private Long deletedSizeBytes;
  }
}
