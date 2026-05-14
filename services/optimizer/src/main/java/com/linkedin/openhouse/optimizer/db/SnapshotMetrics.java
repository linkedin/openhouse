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

  private String clusterId;
  private String tableVersion;
  private String tableLocation;
  private Long tableSizeBytes;

  /** Total number of data files as of the latest snapshot — used for bin-packing. */
  private Long numCurrentFiles;
}
