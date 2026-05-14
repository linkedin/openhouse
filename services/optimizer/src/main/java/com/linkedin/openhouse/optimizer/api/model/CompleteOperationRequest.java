package com.linkedin.openhouse.optimizer.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for {@code POST /v1/table-operations/{id}/complete}.
 *
 * <p>Reports the outcome of a single completed operation. The path's {@code id} is the per-cycle
 * operation UUID — the service looks up that one row and writes a history entry for it.
 *
 * <p>A single Spark job typically processes N tables and yields N independent (status, result)
 * pairs — one per operation. Callers issue one complete request per operation; the service does not
 * bulk-complete by job.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CompleteOperationRequest {

  private String operationId;

  /** Terminal outcome for this single operation. */
  private HistoryStatus status;
}
