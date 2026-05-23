package com.linkedin.openhouse.optimizer.scheduler;

import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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

  @Getter private final OperationTypeDto operationType;
  @Getter private final List<TableOperationDto> operations;

  /** Operation UUIDs in this bin, parallel to {@link #getTableNames()}. */
  public List<String> getOperationIds() {
    return operations.stream().map(TableOperationDto::getId).collect(Collectors.toList());
  }

  /** Fully-qualified {@code database.table} identifiers for the operations in this bin. */
  public List<String> getTableNames() {
    return operations.stream()
        .map(op -> op.getDatabaseName() + "." + op.getTableName())
        .collect(Collectors.toList());
  }

  /**
   * Return a new {@link Bin} containing only the operations whose IDs are in {@code keepIds}. Used
   * by the scheduler to narrow the bin to the rows it actually claimed before launching the job.
   */
  public Bin subset(Collection<String> keepIds) {
    Set<String> keep = new HashSet<>(keepIds);
    List<TableOperationDto> filtered =
        operations.stream().filter(op -> keep.contains(op.getId())).collect(Collectors.toList());
    return new Bin(operationType, filtered);
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
