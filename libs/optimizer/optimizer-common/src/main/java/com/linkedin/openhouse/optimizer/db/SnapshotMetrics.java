package com.linkedin.openhouse.optimizer.db;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Point-in-time snapshot fields. Serialized as JSON into the {@code snapshot} column. */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SnapshotMetrics {

  /** Iceberg metadata version pointer for this snapshot. */
  private String tableVersion;

  /** Filesystem path (or URI) of the table's storage root. */
  private String tableLocation;

  /** Total on-disk size of the table at this snapshot, in bytes. */
  private Long tableSizeBytes;

  /** Total number of data files as of the latest snapshot — used for bin-packing. */
  private Long numCurrentFiles;

  /** Convert to the Spring-free optimizer model. */
  public com.linkedin.openhouse.optimizer.model.TableStatsDto.SnapshotMetrics toModel() {
    return com.linkedin.openhouse.optimizer.model.TableStatsDto.SnapshotMetrics.builder()
        .tableVersion(tableVersion)
        .tableLocation(tableLocation)
        .tableSizeBytes(tableSizeBytes)
        .numCurrentFiles(numCurrentFiles)
        .build();
  }

  /** Build the persistence payload from the Spring-free optimizer model. */
  public static SnapshotMetrics fromModel(
      com.linkedin.openhouse.optimizer.model.TableStatsDto.SnapshotMetrics value) {
    if (value == null) {
      return null;
    }
    return SnapshotMetrics.builder()
        .tableVersion(value.getTableVersion())
        .tableLocation(value.getTableLocation())
        .tableSizeBytes(value.getTableSizeBytes())
        .numCurrentFiles(value.getNumCurrentFiles())
        .build();
  }
}
