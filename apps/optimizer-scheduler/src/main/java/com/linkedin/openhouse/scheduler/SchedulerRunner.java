package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.entity.TableOperationRow;
import com.linkedin.openhouse.optimizer.entity.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import com.linkedin.openhouse.scheduler.client.JobsServiceClient;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * For one operation type per call, reads PENDING rows, enriches them with file count, dispatches to
 * the registered {@link BinPacker}, and submits one Spark job per bin. The {@link
 * com.linkedin.openhouse.scheduler.SchedulerApplication}'s CommandLineRunner loops over the
 * registered packers and invokes {@code schedule(opType)} for each.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SchedulerRunner {

  private final TableOperationsRepository operationsRepo;
  private final TableStatsRepository statsRepo;
  private final JobsServiceClient jobsClient;
  private final Map<OperationType, BinPacker> binPackers;

  @Value("${scheduler.results-endpoint}")
  private String resultsEndpoint;

  /** Schedule all PENDING operations of the given type across all databases. */
  public void schedule(OperationType operationType) {
    schedule(operationType, Optional.empty(), Optional.empty());
  }

  /**
   * Schedule PENDING operations for {@code operationType}, optionally scoped to a single database
   * or table name.
   */
  @Transactional
  public void schedule(
      OperationType operationType, Optional<String> databaseName, Optional<String> tableName) {

    BinPacker packer = binPackers.get(operationType);
    if (packer == null) {
      log.warn("No BinPacker registered for operation type {}; skipping", operationType);
      return;
    }

    List<TableOperationRow> pendingRows =
        operationsRepo.find(
            operationType.name(),
            "PENDING",
            null,
            databaseName.orElse(null),
            tableName.orElse(null));
    if (pendingRows.isEmpty()) {
      log.info("No PENDING operations of type {}; nothing to schedule", operationType);
      return;
    }

    Set<String> uuids =
        pendingRows.stream().map(TableOperationRow::getTableUuid).collect(Collectors.toSet());
    Map<String, Long> fileCountByUuid =
        statsRepo.findAllById(uuids).stream()
            .collect(
                Collectors.toMap(TableStatsRow::getTableUuid, SchedulerRunner::extractFileCount));

    List<TableOperation> pending =
        pendingRows.stream()
            .map(
                row -> {
                  TableOperation op = TableOperation.from(row);
                  op.setFileCount(fileCountByUuid.getOrDefault(row.getTableUuid(), 0L));
                  return op;
                })
            .collect(Collectors.toList());

    List<List<TableOperation>> bins = packer.pack(pending);
    log.info(
        "Packed {} PENDING {} operations into {} bins", pending.size(), operationType, bins.size());

    bins.forEach(bin -> submitBin(operationType, bin));
  }

  private void submitBin(OperationType operationType, List<TableOperation> bin) {
    // Deduplicate PENDING rows per tableUuid for this op type, keeping the IDs in this bin.
    List<String> keepIds = bin.stream().map(TableOperation::getId).collect(Collectors.toList());
    operationsRepo.cancelDuplicatePendingBatch(operationType.name(), keepIds);

    // Claim the rows in one batched UPDATE: PENDING → SCHEDULING.
    int claimedCount = operationsRepo.markSchedulingBatch(keepIds, Instant.now());
    if (claimedCount == 0) {
      log.info("All rows in bin already claimed by another scheduler instance; skipping");
      return;
    }

    // Submit the job for the rows we just claimed.
    List<String> tableNames =
        bin.stream()
            .map(op -> op.getDatabaseName() + "." + op.getTableName())
            .collect(Collectors.toList());
    String jobName =
        "batched-" + operationType.name().toLowerCase() + "-" + Instant.now().toEpochMilli();
    Optional<String> jobId =
        jobsClient.launch(jobName, operationType.name(), tableNames, keepIds, resultsEndpoint);

    if (jobId.isPresent()) {
      int updated = operationsRepo.markScheduledBatch(keepIds, jobId.get());
      log.info(
          "Submitted job {} for {} tables ({} rows marked SCHEDULED)",
          jobId.get(),
          tableNames.size(),
          updated);
    } else {
      int reverted = operationsRepo.markPendingBatch(keepIds);
      log.warn(
          "Job submission failed; reverted {} claimed rows back to PENDING for retry on the next"
              + " pass",
          reverted);
    }
  }

  private static long extractFileCount(TableStatsRow row) {
    if (row.getStats() == null || row.getStats().getSnapshot() == null) {
      return 0L;
    }
    Long count = row.getStats().getSnapshot().getNumCurrentFiles();
    return count != null ? count : 0L;
  }
}
