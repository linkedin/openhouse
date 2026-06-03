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
    Comparator<TableOperationsRow> oldestFirst =
        Comparator.comparing(TableOperationsRow::getCreatedAt)
            .thenComparing(TableOperationsRow::getId);

    // Per tableUuid, the oldest row (lex-tiebreak on id) is the one we schedule; any others are
    // duplicates we cancel. Both lists are derived independently from the same grouping —
    // scheduling does not wait on cancellation and is not predicated on its outcome.
    Map<String, List<TableOperationsRow>> byTableUuid =
        operationsRepo
            .find(
                Optional.of(type.toDb()),
                Optional.of(OperationStatus.PENDING),
                Optional.empty(),
                databaseName,
                tableName,
                Optional.empty(),
                Optional.empty(),
                Pageable.unpaged())
            .stream()
            .collect(Collectors.groupingBy(TableOperationsRow::getTableUuid));

    // Cancel the duplicate-per-tableUuid rows as the terminal side effect of the dedup pipeline.
    // The collectingAndThen finisher short-circuits the IN () clause that Hibernate will not run.
    byTableUuid.values().stream()
        .filter(rows -> rows.size() > 1)
        .flatMap(rows -> rows.stream().sorted(oldestFirst).skip(1))
        .map(TableOperationsRow::getId)
        .collect(
            Collectors.collectingAndThen(
                Collectors.toList(),
                ids -> {
                  if (!ids.isEmpty()) {
                    operationsRepo.cancel(ids);
                  }
                  return null;
                }));
    // Read stats, bin pack, and schedule the operations per bin.
    Optional.ofNullable(registry.get(type))
        .ifPresent(
            packer ->
                packer
                    .pack(
                        byTableUuid.values().stream()
                            .map(rows -> rows.stream().min(oldestFirst).orElseThrow())
                            .map(TableOperationDto::fromRow)
                            .collect(Collectors.toList()),
                        statsRepo.findAllById(byTableUuid.keySet()).stream()
                            .collect(
                                Collectors.toMap(
                                    TableStatsRow::getTableUuid, TableStatsDto::fromRow)))
                    .stream()
                    .map(grouping -> new Bin(type, grouping))
                    .forEach(this::scheduleBin));
  }

  /**
   * Claim the bin's operations, narrow to the rows actually owned, launch one batched Spark job for
   * the claimed subset, and mark SCHEDULED — or revert to PENDING if launch failed.
   */
  @Transactional
  void scheduleBin(Bin bin) {
    Instant claimedAt = Instant.now();
    List<BinItem> claimedItems = claim(bin, claimedAt);

    if (claimedItems.isEmpty()) {
      log.info("All rows in bin already claimed by another scheduler instance; skipping");
      return;
    }
    if (claimedItems.size() < bin.getItems().size()) {
      log.info(
          "Partial claim: {} of {} ops in bin claimed; launching job for claimed subset only",
          claimedItems.size(),
          bin.getItems().size());
    }

    List<String> claimedIds =
        claimedItems.stream().map(BinItem::getOperationId).collect(Collectors.toList());

    jobsClient
        .launch(
            String.format(
                "batched-%s-%d",
                bin.getOperationType().name().toLowerCase(), claimedAt.toEpochMilli()),
            bin.getOperationType().name(),
            claimedItems.stream()
                .map(BinItem::getFullyQualifiedTableName)
                .collect(Collectors.toList()),
            claimedIds,
            resultsEndpoint)
        .ifPresentOrElse(
            jobId -> {
              int updated =
                  operationsRepo.updateBatch(
                      claimedIds,
                      OperationStatus.SCHEDULING,
                      OperationStatus.SCHEDULED,
                      Optional.empty(),
                      Optional.of(jobId));
              log.info(
                  "Submitted job {} for {} tables ({} rows marked SCHEDULED)",
                  jobId,
                  claimedItems.size(),
                  updated);
            },
            () -> {
              int reverted =
                  operationsRepo.updateBatch(
                      claimedIds,
                      OperationStatus.SCHEDULING,
                      OperationStatus.PENDING,
                      Optional.empty(),
                      Optional.empty());
              log.warn(
                  "Job submission failed; reverted {} claimed rows back to PENDING for retry on the"
                      + " next pass",
                  reverted);
            });
  }

  /**
   * CAS-claim every item in the bin from PENDING to SCHEDULING with {@code claimedAt} as the
   * watermark, then narrow {@link Bin#getItems()} to those rows this caller actually owns. Items
   * lost to a racing scheduler are dropped. Returns the {@link BinItem}s ready for launch.
   */
  private List<BinItem> claim(Bin bin, Instant claimedAt) {
    List<String> ids =
        bin.getItems().stream().map(BinItem::getOperationId).collect(Collectors.toList());
    operationsRepo.updateBatch(
        ids,
        OperationStatus.PENDING,
        OperationStatus.SCHEDULING,
        Optional.of(claimedAt),
        Optional.empty());
    Set<String> claimedIds =
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
            .collect(Collectors.toSet());
    return bin.getItems().stream()
        .filter(item -> claimedIds.contains(item.getOperationId()))
        .collect(Collectors.toList());
  }
}
