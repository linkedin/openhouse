package com.linkedin.openhouse.optimizer.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Denormalized stats snapshot captured by the Analyzer at analysis time.
 *
 * <p>Stored as JSON in the {@code metrics} column of {@code table_operations}. These values are
 * point-in-time snapshots — they record what the Analyzer saw when it recommended the operation,
 * not cumulative totals.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OperationMetrics {

  private Long tableSizeBytes;
  private Integer numFilesAdded;
  private Integer numFilesDeleted;
}
