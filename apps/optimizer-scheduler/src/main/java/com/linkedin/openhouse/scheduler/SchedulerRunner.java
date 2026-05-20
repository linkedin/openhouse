package com.linkedin.openhouse.scheduler;

import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
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
  private final Map<OperationTypeDto, BinPacker> binPackers;
  private final String resultsEndpoint;

  public SchedulerRunner(
      TableOperationsRepository operationsRepo,
      TableStatsRepository statsRepo,
      JobsServiceClient jobsClient,
      Map<OperationTypeDto, BinPacker> binPackers,
      @Value("${scheduler.results-endpoint}") String resultsEndpoint) {
    this.operationsRepo = operationsRepo;
    this.statsRepo = statsRepo;
    this.jobsClient = jobsClient;
    this.binPackers = binPackers;
    this.resultsEndpoint = resultsEndpoint;
  }

  /** Schedule all PENDING operations of the given type across all databases. */
  public void schedule(OperationTypeDto operationType) {
    schedule(operationType, Optional.empty(), Optional.empty());
  }

  /**
   * Schedule PENDING operations for {@code operationType}, optionally scoped to a single database
   * or table name.
   */
  @Transactional
  public void schedule(
      OperationTypeDto operationType, Optional<String> databaseName, Optional<String> tableName) {

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

    List<TableOperationDto> pending =
        pendingRows.stream().map(TableOperationDto::fromRow).collect(Collectors.toList());

    // Tradeoff: we fetch fresh table_stats per scheduling cycle (one batched query) rather than
    // denormalizing the relevant fields onto TableOperationDto. The denormalized alternative would
    // remove the per-cycle lookup but widen the TableOperationDto row and serve staler data; the
    // current shape favors smaller operations + freshness over fewer queries.
    Set<String> uuids =
        pending.stream().map(TableOperationDto::getTableUuid).collect(Collectors.toSet());
    Map<String, TableStatsDto> statsByUuid =
        statsRepo.findAllById(uuids).stream()
            .collect(Collectors.toMap(TableStatsRow::getTableUuid, TableStatsDto::fromRow));

    // Filter at the boundary so SchedulingCandidate.stats is guaranteed non-null. A table without
    // a stats row gets skipped this cycle and reconsidered after stats land.
    List<TableOperationDto> withStats =
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

    // Claim the rows in one batched UPDATE: PENDING → SCHEDULING. The UPDATE's row count is just
    // an aggregate — to know *which* rows we own, re-query for SCHEDULING rows tagged with our
    // scheduledAt watermark. Anything not in that subset belongs to another instance or was
    // canceled, and must not be submitted or marked SCHEDULED.
    Instant claimedAt = Instant.now();
    operationsRepo.markSchedulingBatch(ids, claimedAt);
    List<String> claimedIds = operationsRepo.findClaimedIds(ids, claimedAt);
    if (claimedIds.isEmpty()) {
      log.info("All rows in bin already claimed by another scheduler instance; skipping");
      return;
    }
    if (claimedIds.size() < ids.size()) {
      log.info(
          "Partial claim: {} of {} ops in bin claimed; launching job for claimed subset only",
          claimedIds.size(),
          ids.size());
    }

    Bin claimedBin = bin.subset(claimedIds);
    Optional<String> jobId = claimedBin.schedule(jobsClient, resultsEndpoint);
    if (jobId.isPresent()) {
      int updated = operationsRepo.markScheduledBatch(claimedIds, jobId.get());
      log.info(
          "Submitted job {} for {} tables ({} rows marked SCHEDULED)",
          jobId.get(),
          claimedBin.getOperations().size(),
          updated);
    } else {
      int reverted = operationsRepo.markPendingBatch(claimedIds);
      log.warn(
          "Job submission failed; reverted {} claimed rows back to PENDING for retry on the next"
              + " pass",
          reverted);
    }
  }
}
