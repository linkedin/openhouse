package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.TableOperation;
import com.linkedin.openhouse.optimizer.model.TableStats;
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
 * For one operation type per call, reads PENDING rows, looks up per-table stats, dispatches to the
 * registered {@link BinPacker}, and submits one Spark job per returned {@link Bin}. The {@link
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

    List<TableOperationsRow> pendingRows =
        operationsRepo.find(
            operationType.toDb(),
            OperationStatus.PENDING,
            null,
            databaseName.orElse(null),
            tableName.orElse(null));
    if (pendingRows.isEmpty()) {
      log.info("No PENDING operations of type {}; nothing to schedule", operationType);
      return;
    }

    List<TableOperation> pending =
        pendingRows.stream().map(TableOperation::fromRow).collect(Collectors.toList());

    Set<String> uuids =
        pending.stream().map(TableOperation::getTableUuid).collect(Collectors.toSet());
    Map<String, TableStats> statsByUuid =
        statsRepo.findAllById(uuids).stream()
            .collect(Collectors.toMap(TableStatsRow::getTableUuid, TableStats::fromRow));

    List<Bin> bins = packer.pack(pending, statsByUuid);
    log.info(
        "Packed {} PENDING {} operations into {} bins", pending.size(), operationType, bins.size());

    bins.forEach(this::submitBin);
  }

  private void submitBin(Bin bin) {
    List<String> ids = bin.getOperationIds();

    // Deduplicate PENDING rows per tableUuid for this op type, keeping the IDs in this bin.
    operationsRepo.cancelDuplicatePendingBatch(bin.getOperationType().toDb(), ids);

    // Claim the rows in one batched UPDATE: PENDING → SCHEDULING.
    int claimedCount = operationsRepo.markSchedulingBatch(ids, Instant.now());
    if (claimedCount == 0) {
      log.info("All rows in bin already claimed by another scheduler instance; skipping");
      return;
    }

    Optional<String> jobId = bin.schedule(jobsClient, resultsEndpoint);
    if (jobId.isPresent()) {
      int updated = operationsRepo.markScheduledBatch(ids, jobId.get());
      log.info(
          "Submitted job {} for {} tables ({} rows marked SCHEDULED)",
          jobId.get(),
          bin.getOperations().size(),
          updated);
    } else {
      int reverted = operationsRepo.markPendingBatch(ids);
      log.warn(
          "Job submission failed; reverted {} claimed rows back to PENDING for retry on the next"
              + " pass",
          reverted);
    }
  }
}
