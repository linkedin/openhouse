package com.linkedin.openhouse.jobs.spark.optimizer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wire-compatible body for {@code POST /v1/optimizer/operations/update} on the Optimizer Service.
 *
 * <p>Mirrors {@code com.linkedin.openhouse.optimizer.api.spec.UpdateOperationRequest} from the
 * optimizer service module so this app can be built before that module merges. Keep the two in
 * sync.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OperationUpdateRequest {
  private String operationId;
  private String status;
  private String tableUuid;
  private String databaseName;
  private String tableName;
  private String operationType;
}
