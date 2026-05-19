package com.linkedin.openhouse.optimizer.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for {@code POST /v1/table-operations/complete}.
 *
 * <p>Reports the outcome of a single completed operation. The service looks up the operation row by
 * {@link #operationId} and writes a history entry for it.
 *
 * <p>A single Spark job typically processes N tables and yields N independent (status) outcomes —
 * one per operation. Callers issue one complete request per operation; the service does not
 * bulk-complete by job.
 *
 * <p>The remaining fields ({@link #tableUuid}, {@link #databaseName}, {@link #tableName}, {@link
 * #operationType}) are debug-only echo information. The server does not key off them; they are
 * preserved on log lines and traces so an operator looking at a failing complete call can see which
 * (db, table, operation) the caller believed it was completing without joining back to the
 * operation row.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CompleteOperationRequestDto {

  /** Operation row's UUID — the primary lookup key. */
  private String operationId;

  /** Terminal outcome for this single operation. */
  private HistoryStatusDto status;

  /** Debug echo: stable table identity the caller believed it was completing. */
  private String tableUuid;

  /** Debug echo: database name. */
  private String databaseName;

  /** Debug echo: table name. */
  private String tableName;

  /** Debug echo: operation type. */
  private OperationTypeDto operationType;
}
