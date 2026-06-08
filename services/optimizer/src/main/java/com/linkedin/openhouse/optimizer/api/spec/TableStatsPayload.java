package com.linkedin.openhouse.optimizer.api.spec;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Combined stats payload exposed on the optimizer wire API.
 *
 * <p>API-layer copy of the stats payload — self-contained, evolved only when the wire contract
 * changes.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableStatsPayload {

  /** Snapshot fields — overwritten on every upsert. */
  private SnapshotMetricsDto snapshot;

  /** Delta fields — accumulated across commit events. */
  private CommitDeltaDto delta;

  /** Convert to the internal-model counterpart. */
  public com.linkedin.openhouse.optimizer.model.TableStatsDto toModel() {
    return com.linkedin.openhouse.optimizer.model.TableStatsDto.builder()
        .snapshot(snapshot == null ? null : snapshot.toModel())
        .delta(delta == null ? null : delta.toModel())
        .build();
  }

  /** Build the api-layer payload from the internal-model counterpart. */
  public static TableStatsPayload fromModel(
      com.linkedin.openhouse.optimizer.model.TableStatsDto m) {
    if (m == null) {
      return null;
    }
    return TableStatsPayload.builder()
        .snapshot(SnapshotMetricsDto.fromModel(m.getSnapshot()))
        .delta(CommitDeltaDto.fromModel(m.getDelta()))
        .build();
  }

  /** Point-in-time metadata read from Iceberg at scan time. */
  @Data
  @Builder(toBuilder = true)
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SnapshotMetricsDto {

    /** Iceberg metadata version pointer for this snapshot. */
    private String tableVersion;

    /** Filesystem path (or URI) of the table's storage root. */
    private String tableLocation;

    /** Total on-disk size of the table at this snapshot, in bytes. */
    private Long tableSizeBytes;

    /** Total number of data files as of the latest snapshot — used for bin-packing. */
    private Long numCurrentFiles;

    /** Convert to the internal-model counterpart. */
    public com.linkedin.openhouse.optimizer.model.TableStatsDto.SnapshotMetrics toModel() {
      return com.linkedin.openhouse.optimizer.model.TableStatsDto.SnapshotMetrics.builder()
          .tableVersion(tableVersion)
          .tableLocation(tableLocation)
          .tableSizeBytes(tableSizeBytes)
          .numCurrentFiles(numCurrentFiles)
          .build();
    }

    /** Build the api-layer inner object from the internal-model counterpart. */
    public static SnapshotMetricsDto fromModel(
        com.linkedin.openhouse.optimizer.model.TableStatsDto.SnapshotMetrics m) {
      if (m == null) {
        return null;
      }
      return SnapshotMetricsDto.builder()
          .tableVersion(m.getTableVersion())
          .tableLocation(m.getTableLocation())
          .tableSizeBytes(m.getTableSizeBytes())
          .numCurrentFiles(m.getNumCurrentFiles())
          .build();
    }
  }

  /** Per-commit incremental counters; accumulated across all recorded commit events. */
  @Data
  @Builder(toBuilder = true)
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class CommitDeltaDto {

    /** Number of data files this commit added to the table. */
    private Long numFilesAdded;

    /** Number of data files this commit removed from the table. */
    private Long numFilesDeleted;

    /** Total bytes added by this commit. */
    private Long addedSizeBytes;

    /** Total bytes removed by this commit. */
    private Long deletedSizeBytes;

    /** Convert to the internal-model counterpart. */
    public com.linkedin.openhouse.optimizer.model.TableStatsDto.CommitDelta toModel() {
      return com.linkedin.openhouse.optimizer.model.TableStatsDto.CommitDelta.builder()
          .numFilesAdded(numFilesAdded)
          .numFilesDeleted(numFilesDeleted)
          .addedSizeBytes(addedSizeBytes)
          .deletedSizeBytes(deletedSizeBytes)
          .build();
    }

    /** Build the api-layer inner object from the internal-model counterpart. */
    public static CommitDeltaDto fromModel(
        com.linkedin.openhouse.optimizer.model.TableStatsDto.CommitDelta m) {
      if (m == null) {
        return null;
      }
      return CommitDeltaDto.builder()
          .numFilesAdded(m.getNumFilesAdded())
          .numFilesDeleted(m.getNumFilesDeleted())
          .addedSizeBytes(m.getAddedSizeBytes())
          .deletedSizeBytes(m.getDeletedSizeBytes())
          .build();
    }
  }
}
