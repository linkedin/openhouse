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

  private Long numFilesAdded;
  private Long numFilesDeleted;
  private Long addedSizeBytes;
  private Long deletedSizeBytes;
}
