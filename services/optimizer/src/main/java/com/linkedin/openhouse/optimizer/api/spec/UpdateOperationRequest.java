package com.linkedin.openhouse.optimizer.api.spec;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for {@code POST /v1/table-operations/update}.
 *
 * <p>Reports the outcome of a single operation update. The service looks up the operation row by
 * {@link #operationId} and writes a history entry for it.
 *
 * <p>A single Spark job typically processes N tables and yields N independent (status) outcomes —
 * one per operation. Callers issue one update request per operation; the service does not
 * bulk-update by job.
 *
 * <p>The remaining fields ({@link #tableUuid}, {@link #databaseName}, {@link #tableName}, {@link
 * #operationType}) are debug-only echo information. The server does not key off them; they are
 * preserved on log lines and traces so an operator looking at a failing update call can see which
 * (db, table, operation) the caller believed it was updating without joining back to the operation
 * row.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpdateOperationRequest {

  /**
   * Operation row's UUID. Required. Must match the {@code {id}} path variable on {@code POST
   * /v1/optimizer/operations/{id}/update} — the controller rejects mismatched requests with 400.
   * Carrying it in the body keeps the payload self-describing for trace/log consumers that may not
   * see the URL.
   */
  @NotBlank private String operationId;

  /** Terminal outcome for this single operation. Required. */
  @NotNull private HistoryStatus status;

  /** Debug echo: stable table identity the caller believed it was completing. */
  private String tableUuid;

  /** Debug echo: database name. */
  private String databaseName;

  /** Debug echo: table name. */
  private String tableName;

  /** Debug echo: operation type. */
  private OperationType operationType;
}
