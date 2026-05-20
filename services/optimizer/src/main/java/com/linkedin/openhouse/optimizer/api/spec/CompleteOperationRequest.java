package com.linkedin.openhouse.optimizer.api.spec;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request body for {@code POST /v1/optimizer/operations/{id}/complete}.
 *
 * <p>Reports the outcome of a single completed operation. The operation row's UUID lives in the URL
 * path; the body carries the terminal status and any operation-type-specific result metrics.
 *
 * <p>A single Spark job typically processes N tables and yields N independent (status) outcomes —
 * one per operation. Callers issue one complete request per operation; the service does not
 * bulk-complete by job.
 *
 * <p>The debug-echo fields ({@link #tableUuid}, {@link #databaseName}, {@link #tableName}, {@link
 * #operationType}) are optional. The server does not key off them; they are preserved on log lines
 * and traces so an operator looking at a failing complete call can see which (db, table, operation)
 * the caller believed it was completing without joining back to the operation row.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CompleteOperationRequest {

  /** Terminal outcome for this single operation. */
  private HistoryStatus status;

  /** OFD-specific: number of orphan files deleted. Null on failure or non-OFD operation. */
  private Long orphanFilesDeleted;

  /** OFD-specific: bytes reclaimed. Null on failure or non-OFD operation. */
  private Long orphanBytesDeleted;

  /** On failure, the exception message from the Spark-side worker. Null on success. */
  private String errorMessage;

  /** On failure, the simple name of the exception class. Null on success. */
  private String errorType;

  /** Debug echo: stable table identity the caller believed it was completing. */
  private String tableUuid;

  /** Debug echo: database name. */
  private String databaseName;

  /** Debug echo: table name. */
  private String tableName;

  /** Debug echo: operation type. */
  private OperationType operationType;
}
