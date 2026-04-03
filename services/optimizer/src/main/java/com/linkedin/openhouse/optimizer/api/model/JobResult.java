package com.linkedin.openhouse.optimizer.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Result payload for a completed Spark maintenance job.
 *
 * <p>Stored as JSON in the {@code result} column of {@code table_operations_history}. Both fields
 * are {@code null} on success; populated on failure.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobResult {

  /** Human-readable error message; {@code null} if the job succeeded. */
  private String errorMessage;

  /** Error category (e.g., {@code OOM}, {@code TIMEOUT}); {@code null} if the job succeeded. */
  private String errorType;
}
