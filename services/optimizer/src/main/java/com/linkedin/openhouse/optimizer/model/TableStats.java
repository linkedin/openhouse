package com.linkedin.openhouse.optimizer.model;

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

  /** Project to the DB-layer {@link com.linkedin.openhouse.optimizer.db.SnapshotMetrics} object. */
  public com.linkedin.openhouse.optimizer.db.SnapshotMetrics toSnapshotRow() {
    return snapshot == null ? null : snapshot.toDb();
  }

  /**
   * Project to the DB-layer {@link com.linkedin.openhouse.optimizer.db.CommitDeltaMetrics} object.
   */
  public com.linkedin.openhouse.optimizer.db.CommitDeltaMetrics toDeltaRow() {
    return delta == null ? null : delta.toDb();
  }

  /** Join the two DB-side columns back into a single internal-model {@link TableStats}. */
  public static TableStats fromRows(
      com.linkedin.openhouse.optimizer.db.SnapshotMetrics dbSnapshot,
      com.linkedin.openhouse.optimizer.db.CommitDeltaMetrics dbDelta) {
    if (dbSnapshot == null && dbDelta == null) {
      return null;
    }
    return TableStats.builder()
        .snapshot(SnapshotMetrics.fromDb(dbSnapshot))
        .delta(CommitDelta.fromDb(dbDelta))
        .build();
  }

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

    /** Convert to the DB-layer counterpart. */
    public com.linkedin.openhouse.optimizer.db.SnapshotMetrics toDb() {
      return com.linkedin.openhouse.optimizer.db.SnapshotMetrics.builder()
          .tableVersion(tableVersion)
          .tableLocation(tableLocation)
          .tableSizeBytes(tableSizeBytes)
          .numCurrentFiles(numCurrentFiles)
          .build();
    }

    /** Build the internal-model inner object from the DB-layer counterpart. */
    public static SnapshotMetrics fromDb(com.linkedin.openhouse.optimizer.db.SnapshotMetrics v) {
      if (v == null) {
        return null;
      }
      return SnapshotMetrics.builder()
          .tableVersion(v.getTableVersion())
          .tableLocation(v.getTableLocation())
          .tableSizeBytes(v.getTableSizeBytes())
          .numCurrentFiles(v.getNumCurrentFiles())
          .build();
    }
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

    /** Convert to the DB-layer counterpart. */
    public com.linkedin.openhouse.optimizer.db.CommitDeltaMetrics toDb() {
      return com.linkedin.openhouse.optimizer.db.CommitDeltaMetrics.builder()
          .numFilesAdded(numFilesAdded)
          .numFilesDeleted(numFilesDeleted)
          .addedSizeBytes(addedSizeBytes)
          .deletedSizeBytes(deletedSizeBytes)
          .build();
    }

    /** Build the internal-model inner object from the DB-layer counterpart. */
    public static CommitDelta fromDb(com.linkedin.openhouse.optimizer.db.CommitDeltaMetrics v) {
      if (v == null) {
        return null;
      }
      return CommitDelta.builder()
          .numFilesAdded(v.getNumFilesAdded())
          .numFilesDeleted(v.getNumFilesDeleted())
          .addedSizeBytes(v.getAddedSizeBytes())
          .deletedSizeBytes(v.getDeletedSizeBytes())
          .build();
    }
  }
}
