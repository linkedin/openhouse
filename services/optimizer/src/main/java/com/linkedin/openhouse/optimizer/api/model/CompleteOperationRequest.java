package com.linkedin.openhouse.optimizer.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for {@code POST /v1/table-operations/{id}/complete}.
 *
 * <p>Reports the outcome of a completed operation. The backend looks up the operation row by {@code
 * id} and writes a history entry with the operation's table metadata and the supplied result.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CompleteOperationRequest {

  /** Outcome of the operation. */
  private OperationHistoryStatus status;

  /** Error details on failure; {@code null} on success. */
  private JobResult result;
}
