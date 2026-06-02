package com.linkedin.openhouse.optimizer.scheduler;

import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableStatsDto;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import com.linkedin.openhouse.optimizer.scheduler.binpack.Bin;
import com.linkedin.openhouse.optimizer.scheduler.binpack.BinItem;
import com.linkedin.openhouse.optimizer.scheduler.binpack.BinPacker;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

/**
 * Generic scheduler. Operation types are registered at construction via {@link #registerOperation},
 * which returns a new instance with the additional entry — the registry is immutable, so the bean
 * Spring publishes is the fully-registered runner produced in {@link
 * com.linkedin.openhouse.optimizer.scheduler.config.SchedulerConfig}. For each registered type the
 * runner:
 *
 * <ol>
 *   <li>Reads PENDING rows from MySQL.
 *   <li>For each {@code tableUuid}, picks the oldest PENDING row to schedule and the rest to cancel
 *       — both lists derived independently from the same grouping.
 *   <li>Cancels the duplicate rows; loads stats for the rows to schedule.
 *   <li>Hands the (operations, stats) pair to the {@link BinPacker} and receives one grouping per
 *       batch.
 *   <li>Wraps each grouping into a {@link Bin} tagged with the operation type and schedules it
 *       (claim CAS, narrow to claimed, launch, record).
 * </ol>
 *
 * <p>The runner is operation-agnostic. All IO and the claim/launch/mark lifecycle live here; the
 * only per-operation knowledge in the module is the {@link BinPacker} the caller registers.
 */
@Slf4j
public class SchedulerRunner {

  private final TableOperationsRepository operationsRepo;
  private final TableStatsRepository statsRepo;
  private final JobsServiceClient jobsClient;
  private final String resultsEndpoint;
  private final Map<OperationTypeDto, BinPacker> registry;

  public SchedulerRunner(
      TableOperationsRepository operationsRepo,
      TableStatsRepository statsRepo,
      JobsServiceClient jobsClient,
      String resultsEndpoint) {
    this(operationsRepo, statsRepo, jobsClient, resultsEndpoint, Map.of());
  }

  private SchedulerRunner(
      TableOperationsRepository operationsRepo,
      TableStatsRepository statsRepo,
      JobsServiceClient jobsClient,
      String resultsEndpoint,
      Map<OperationTypeDto, BinPacker> registry) {
    this.operationsRepo = operationsRepo;
    this.statsRepo = statsRepo;
    this.jobsClient = jobsClient;
    this.resultsEndpoint = resultsEndpoint;
    this.registry = registry;
  }

  /**
   * Return a new {@link SchedulerRunner} whose registry is this one's plus {@code (type, packer)}.
   * If {@code type} was already registered, the new entry replaces the prior one. Pure: the
   * receiver is unchanged.
   */
  public SchedulerRunner registerOperation(OperationTypeDto type, BinPacker packer) {
    HashMap<OperationTypeDto, BinPacker> next = new HashMap<>(registry);
    next.put(type, packer);
    return new SchedulerRunner(
        operationsRepo, statsRepo, jobsClient, resultsEndpoint, Map.copyOf(next));
  }

  public Set<OperationTypeDto> getRegisteredOperationTypes() {
    return registry.keySet();
  }

  public void schedule(OperationTypeDto type) {
    schedule(type, Optional.empty(), Optional.empty());
  }

  public void schedule(
      OperationTypeDto type, Optional<String> databaseName, Optional<String> tableName) {
    BinPacker packer =
        Optional.ofNullable(registry.get(type))
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No BinPacker registered for operation type " + type));

    List<TableOperationsRow> pending =
        operationsRepo.find(
            Optional.of(type.toDb()),
            Optional.of(OperationStatus.PENDING),
            Optional.empty(),
            databaseName,
            tableName,
            Optional.empty(),
            Optional.empty(),
            Pageable.unpaged());
    if (pending.isEmpty()) {
      log.info("No PENDING operations of type {}; nothing to schedule", type);
      return;
    }

    // Per tableUuid, the oldest row (lex-tiebreak on id) is the one we schedule; any others are
    // duplicates we cancel. Both lists are derived independently from the same grouping —
    // scheduling does not wait on cancellation and is not predicated on its outcome.
    Map<String, List<TableOperationsRow>> byTableUuid =
        pending.stream().collect(Collectors.groupingBy(TableOperationsRow::getTableUuid));
    Comparator<TableOperationsRow> oldestFirst =
        Comparator.comparing(TableOperationsRow::getCreatedAt)
            .thenComparing(TableOperationsRow::getId);
    List<TableOperationDto> toSchedule =
        byTableUuid.values().stream()
            .map(rows -> rows.stream().min(oldestFirst).orElseThrow())
            .map(TableOperationDto::fromRow)
            .collect(Collectors.toList());

    int cancelled =
        operationsRepo.cancel(
            byTableUuid.values().stream()
                .filter(rows -> rows.size() > 1)
                .flatMap(rows -> rows.stream().sorted(oldestFirst).skip(1))
                .map(TableOperationsRow::getId)
                .collect(Collectors.toList()));
    if (cancelled > 0) {
      log.warn("Cancelled {} duplicate PENDING rows", cancelled);
    }

    Map<String, TableStatsDto> statsByUuid =
        statsRepo.findAllById(byTableUuid.keySet()).stream()
            .collect(Collectors.toMap(TableStatsRow::getTableUuid, TableStatsDto::fromRow));

    List<Bin> bins =
        packer.pack(toSchedule, statsByUuid).stream()
            .map(grouping -> new Bin(type, grouping))
            .collect(Collectors.toList());
    log.info("Packed {} PENDING {} operations into {} bins", toSchedule.size(), type, bins.size());

    bins.forEach(this::scheduleBin);
  }

  /**
   * Claim the bin's operations, narrow to the rows actually owned, launch one batched Spark job for
   * the claimed subset, and mark SCHEDULED — or revert to PENDING if launch failed.
   */
  @Transactional
  void scheduleBin(Bin bin) {
    List<String> ids =
        bin.getItems().stream().map(BinItem::getOperationId).collect(Collectors.toList());

    Instant claimedAt = Instant.now();
    operationsRepo.updateBatch(
        ids,
        OperationStatus.PENDING,
        OperationStatus.SCHEDULING,
        Optional.of(claimedAt),
        Optional.empty());
    List<String> claimedIds =
        operationsRepo
            .find(
                Optional.empty(),
                Optional.of(OperationStatus.SCHEDULING),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(claimedAt),
                Optional.of(ids),
                Pageable.unpaged())
            .stream()
            .map(TableOperationsRow::getId)
            .collect(Collectors.toList());
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

    Set<String> claimedSet = new HashSet<>(claimedIds);
    List<BinItem> claimedItems =
        bin.getItems().stream()
            .filter(item -> claimedSet.contains(item.getOperationId()))
            .collect(Collectors.toList());
    List<String> tableNames =
        claimedItems.stream().map(BinItem::getFullyQualifiedTableName).collect(Collectors.toList());
    List<String> operationIds =
        claimedItems.stream().map(BinItem::getOperationId).collect(Collectors.toList());

    String jobName =
        "batched-" + bin.getOperationType().name().toLowerCase() + "-" + claimedAt.toEpochMilli();
    Optional<String> jobId =
        jobsClient.launch(
            jobName, bin.getOperationType().name(), tableNames, operationIds, resultsEndpoint);

    if (jobId.isPresent()) {
      int updated =
          operationsRepo.updateBatch(
              claimedIds,
              OperationStatus.SCHEDULING,
              OperationStatus.SCHEDULED,
              Optional.empty(),
              Optional.of(jobId.get()));
      log.info(
          "Submitted job {} for {} tables ({} rows marked SCHEDULED)",
          jobId.get(),
          claimedItems.size(),
          updated);
    } else {
      int reverted =
          operationsRepo.updateBatch(
              claimedIds,
              OperationStatus.SCHEDULING,
              OperationStatus.PENDING,
              Optional.empty(),
              Optional.empty());
      log.warn(
          "Job submission failed; reverted {} claimed rows back to PENDING for retry on the next"
              + " pass",
          reverted);
    }
  }
}
