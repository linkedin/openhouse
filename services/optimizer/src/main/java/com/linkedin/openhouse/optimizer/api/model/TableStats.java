package com.linkedin.openhouse.optimizer.api.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Combined stats payload stored as a single JSON blob per table. */
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
    /** Total number of data files as of the latest snapshot — used for bin-packing. */
    private Long numCurrentFiles;
  }

  /** Per-commit incremental counters; accumulated across all recorded commit events. */
  @Data
  @Builder(toBuilder = true)
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CommitDelta {
    private Long numFilesAdded;
    private Long numFilesDeleted;
    private Long addedSizeBytes;
    private Long deletedSizeBytes;
  }
}
