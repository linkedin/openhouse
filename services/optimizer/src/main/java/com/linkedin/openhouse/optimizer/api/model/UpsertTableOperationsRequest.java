package com.linkedin.openhouse.optimizer.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * PUT request body for {@code /v1/table-operations/{id}}.
 *
 * <p>The Analyzer supplies the operation {@code id} (client-generated UUID) in the path and all
 * table-identifying fields in this body. The service creates the row on first call.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpsertTableOperationsRequest {

  /** Stable Iceberg table UUID identifying the target table. */
  private String tableUuid;

  /** Denormalized database name for display. */
  private String databaseName;

  /** Denormalized table name for display. */
  private String tableName;

  /** The type of maintenance operation to create. */
  private OperationType operationType;
}
