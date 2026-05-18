package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.scheduler.client.JobsServiceClient;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A set of operations the scheduler will submit together as a single Spark job. A bin owns its own
 * launch — callers ask it to schedule itself and react to the returned job id. The surrounding
 * status-update machinery (claim, mark-scheduled, revert-to-pending) lives in the scheduler because
 * it is shared across all bins regardless of operation type.
 */
@RequiredArgsConstructor
public class Bin {

  @Getter private final OperationType operationType;
  @Getter private final List<TableOperation> operations;

  /** Operation UUIDs in this bin, parallel to {@link #getTableNames()}. */
  public List<String> getOperationIds() {
    return operations.stream().map(TableOperation::getId).collect(Collectors.toList());
  }

  /** Fully-qualified {@code database.table} identifiers for the operations in this bin. */
  public List<String> getTableNames() {
    return operations.stream()
        .map(op -> op.getDatabaseName() + "." + op.getTableName())
        .collect(Collectors.toList());
  }

  /**
   * Submit this bin as a single Spark job. Returns the job id on success, or empty on submission
   * failure — the caller is responsible for the surrounding status updates.
   */
  public Optional<String> schedule(JobsServiceClient client, String resultsEndpoint) {
    String jobName =
        "batched-" + operationType.name().toLowerCase() + "-" + Instant.now().toEpochMilli();
    return client.launch(
        jobName, operationType.name(), getTableNames(), getOperationIds(), resultsEndpoint);
  }
}
