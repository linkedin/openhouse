package com.linkedin.openhouse.optimizer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Internal-model result payload for a completed Spark maintenance job.
 *
 * <p>Internal-layer copy of the structured result. Both fields are {@code null} on success;
 * populated on failure. Intentionally separate from the wire-API and DB representations.
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
