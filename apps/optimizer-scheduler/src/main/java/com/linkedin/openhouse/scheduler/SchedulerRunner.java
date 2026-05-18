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
public class SchedulerRunner {
  private final TableOperationsRepository operationsRepo;
  private final TableStatsRepository statsRepo;
  private final JobsServiceClient jobsClient;
  private final Map<OperationType, BinPacker> binPackers;
  private final String resultsEndpoint;

  public SchedulerRunner(
      TableOperationsRepository operationsRepo,
      TableStatsRepository statsRepo,
      JobsServiceClient jobsClient,
      Map<OperationType, BinPacker> binPackers,
      @Value("${scheduler.results-endpoint}") String resultsEndpoint) {
    this.operationsRepo = operationsRepo;
    this.statsRepo = statsRepo;
    this.jobsClient = jobsClient;
    this.binPackers = binPackers;
    this.resultsEndpoint = resultsEndpoint;
  }

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
      throw new IllegalStateException(
          "No BinPacker registered for operation type " + operationType);
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

    // Tradeoff: we fetch fresh table_stats per scheduling cycle (one batched query) rather than
    // denormalizing the relevant fields onto TableOperation. The denormalized alternative would
    // remove the per-cycle lookup but widen the TableOperation row and serve staler data; the
    // current shape favors smaller operations + freshness over fewer queries.
    Set<String> uuids =
        pending.stream().map(TableOperation::getTableUuid).collect(Collectors.toSet());
    Map<String, TableStats> statsByUuid =
        statsRepo.findAllById(uuids).stream()
            .collect(Collectors.toMap(TableStatsRow::getTableUuid, TableStats::fromRow));

    // Filter at the boundary so SchedulingCandidate.stats is guaranteed non-null. A table without
    // a stats row gets skipped this cycle and reconsidered after stats land.
    List<TableOperation> withStats =
        pending.stream()
            .filter(op -> statsByUuid.containsKey(op.getTableUuid()))
            .collect(Collectors.toList());
    if (withStats.size() < pending.size()) {
      log.warn(
          "Skipped {} {} operations with no table_stats row",
          pending.size() - withStats.size(),
          operationType);
    }
    if (withStats.isEmpty()) {
      return;
    }

    List<SchedulingCandidate> candidates =
        withStats.stream()
            .map(op -> new SchedulingCandidate(op, statsByUuid.get(op.getTableUuid())))
            .collect(Collectors.toList());

    List<Bin> bins = packer.pack(candidates);
    log.info(
        "Packed {} PENDING {} operations into {} bins",
        candidates.size(),
        operationType,
        bins.size());

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
