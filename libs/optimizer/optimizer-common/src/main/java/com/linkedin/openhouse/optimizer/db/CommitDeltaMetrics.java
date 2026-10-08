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

  /** Convert to the Spring-free optimizer model. */
  public com.linkedin.openhouse.optimizer.model.TableStatsDto.CommitDelta toModel() {
    return com.linkedin.openhouse.optimizer.model.TableStatsDto.CommitDelta.builder()
        .numFilesAdded(numFilesAdded)
        .numFilesDeleted(numFilesDeleted)
        .addedSizeBytes(addedSizeBytes)
        .deletedSizeBytes(deletedSizeBytes)
        .build();
  }

  /** Build the persistence payload from the Spring-free optimizer model. */
  public static CommitDeltaMetrics fromModel(
      com.linkedin.openhouse.optimizer.model.TableStatsDto.CommitDelta value) {
    if (value == null) {
      return null;
    }
    return CommitDeltaMetrics.builder()
        .numFilesAdded(value.getNumFilesAdded())
        .numFilesDeleted(value.getNumFilesDeleted())
        .addedSizeBytes(value.getAddedSizeBytes())
        .deletedSizeBytes(value.getDeletedSizeBytes())
        .build();
  }
}
