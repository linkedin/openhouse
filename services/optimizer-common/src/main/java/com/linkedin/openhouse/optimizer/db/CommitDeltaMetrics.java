package com.linkedin.openhouse.optimizer.db;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Per-commit incremental counters. Serialized as JSON into the {@code delta} column. */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CommitDeltaMetrics {

  /** Number of data files this commit added to the table. */
  private Long numFilesAdded;

  /** Number of data files this commit removed from the table. */
  private Long numFilesDeleted;

  /** Total bytes added by this commit. */
  private Long addedSizeBytes;

  /** Total bytes removed by this commit. */
  private Long deletedSizeBytes;
}
