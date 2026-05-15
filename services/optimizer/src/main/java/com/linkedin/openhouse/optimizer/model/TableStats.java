package com.linkedin.openhouse.optimizer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Self-describing per-table stats record. Carries the table's identity and metadata alongside the
 * snapshot + delta payload so consumers don't need an outer wrapper to know which table the stats
 * belong to.
 *
 * <p>Identity ({@link #tableUuid}, {@link #databaseName}, {@link #tableName}) and metadata ({@link
 * #tableProperties}, {@link #updatedAt}) are populated when read from a current-state row. When
 * this record is built from a per-commit history row, {@link #delta} is populated and {@link
 * #tableProperties} / {@link #updatedAt} are typically {@code null}.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableStats {

  /** Stable table identity from the Tables Service. Survives renames; rotates on drop+recreate. */
  private String tableUuid;

  /** Database the table lives in. */
  private String databaseName;

  /** Iceberg table name (the human-readable identifier, not the UUID). */
  private String tableName;

  /** Current table-property map (e.g. maintenance opt-in flags). Never null. */
  @Builder.Default private Map<String, String> tableProperties = Collections.emptyMap();

  /** Snapshot fields — overwritten on every upsert. */
  private SnapshotMetrics snapshot;

  /** Delta fields — accumulated across commit events. Null when read from a current-state row. */
  private CommitDelta delta;

  /** When the current snapshot was last written. Stamped server-side on every upsert. */
  private Instant updatedAt;

  /** Point-in-time metadata read from Iceberg at scan time. */
  @Data
  @Builder(toBuilder = true)
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SnapshotMetrics {

    /** Iceberg metadata version pointer for this snapshot. */
    private String tableVersion;

    /** Filesystem path (or URI) of the table's storage root. */
    private String tableLocation;

    /** Total on-disk size of the table at this snapshot, in bytes. */
    private Long tableSizeBytes;

    /** Total number of data files as of the latest snapshot — used for bin-packing. */
    private Long numCurrentFiles;
  }

  /** Per-commit incremental counters; accumulated across all recorded commit events. */
  @Data
  @Builder(toBuilder = true)
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class CommitDelta {

    /** Number of data files this commit added to the table. */
    private Long numFilesAdded;

    /** Number of data files this commit removed from the table. */
    private Long numFilesDeleted;

    /** Total bytes added by this commit. */
    private Long addedSizeBytes;

    /** Total bytes removed by this commit. */
    private Long deletedSizeBytes;
  }
}
