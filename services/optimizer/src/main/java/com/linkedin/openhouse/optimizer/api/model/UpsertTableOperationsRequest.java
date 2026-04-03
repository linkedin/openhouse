package com.linkedin.openhouse.optimizer.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * PUT request body for {@code /v1/table-operations/{id}}.
 *
 * <p>The Analyzer supplies the operation {@code id} (client-generated UUID) in the path and all
 * table-identifying fields in this body. The service upserts by {@code id}: creates on first call,
 * updates {@code metrics} on subsequent calls with the same {@code id}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpsertTableOperationsRequest {

  private String tableUuid;
  private String databaseName;
  private String tableName;
  private OperationType operationType;
  private OperationMetrics metrics;
}
