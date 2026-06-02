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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Generic scheduler. For each operation type registered via {@link BinPackerRegistration}:
 *
 * <ol>
 *   <li>Reads PENDING rows from MySQL.
 *   <li>Deduplicates duplicate PENDING rows for the same {@code tableUuid}.
 *   <li>Loads the stats row for every survivor.
 *   <li>Projects each (operation, stats) pair into a {@link BinItem} via the registration's
 *       prototype.
 *   <li>Hands the items to the {@link BinPacker} to get bins.
 *   <li>Schedules each bin (claim CAS, narrow to claimed, launch, record).
 * </ol>
 *
 * <p>The runner is operation-agnostic. All IO and the claim/launch/mark lifecycle live here. The
 * only per-operation knowledge in the module is the {@link BinPackerRegistration} bean wired in
 * {@link com.linkedin.openhouse.optimizer.scheduler.config.SchedulerConfig}.
 */
@Slf4j
@Component
public class SchedulerRunner {

  private final TableOperationsRepository operationsRepo;
  private final TableStatsRepository statsRepo;
  private final JobsServiceClient jobsClient;
  private final String resultsEndpoint;
  private final Map<OperationTypeDto, BinPackerRegistration> registry;

  @Autowired
  public SchedulerRunner(
      TableOperationsRepository operationsRepo,
      TableStatsRepository statsRepo,
      JobsServiceClient jobsClient,
      @Value("${optimizer.scheduler.results-endpoint}") String resultsEndpoint,
      List<BinPackerRegistration> registrations) {
    this.operationsRepo = operationsRepo;
    this.statsRepo = statsRepo;
    this.jobsClient = jobsClient;
    this.resultsEndpoint = resultsEndpoint;
    this.registry =
        Map.copyOf(
            registrations.stream()
                .collect(
                    Collectors.toMap(
                        BinPackerRegistration::getOperationType, Function.identity())));
  }

  public Set<OperationTypeDto> getRegisteredOperationTypes() {
    return registry.keySet();
  }

  public void schedule(OperationTypeDto type) {
    schedule(type, Optional.empty(), Optional.empty());
  }

  public void schedule(
      OperationTypeDto type, Optional<String> databaseName, Optional<String> tableName) {
    BinPackerRegistration reg = registry.get(type);
    if (reg == null) {
      throw new IllegalStateException("No BinPacker registered for operation type " + type);
    }

    List<TableOperationDto> pending = loadAndDedupPending(type, databaseName, tableName);
    if (pending.isEmpty()) {
      return;
    }
    Map<String, TableStatsDto> statsByUuid = loadStatsByUuid(pending);

    List<BinItem> items = projectToItems(pending, statsByUuid, reg.getPrototype(), type);
    if (items.isEmpty()) {
      return;
    }

    List<Bin> bins = reg.getPacker().pack(items);
    log.info("Packed {} PENDING {} operations into {} bins", items.size(), type, bins.size());

    bins.forEach(this::scheduleBin);
  }

  private List<TableOperationDto> loadAndDedupPending(
      OperationTypeDto type, Optional<String> databaseName, Optional<String> tableName) {
    // Unpaged: correctness requires the full PENDING set in one cycle; the working set is bounded
    // by count(PENDING for this op type). Single-page truncation would silently drop work past
    // page 0.
    List<TableOperationsRow> pendingRows =
        operationsRepo.find(
            Optional.of(type.toDb()),
            Optional.of(OperationStatus.PENDING),
            Optional.empty(),
            databaseName,
            tableName,
            Optional.empty(),
            Optional.empty(),
            Pageable.unpaged());
    if (pendingRows.isEmpty()) {
      log.info("No PENDING operations of type {}; nothing to schedule", type);
      return List.of();
    }
    List<TableOperationsRow> survivors = cancelDuplicates(pendingRows);
    return survivors.stream().map(TableOperationDto::fromRow).collect(Collectors.toList());
  }

  /**
   * Group {@code pendingRows} by {@code tableUuid}; for any group with more than one row, cancel
   * all but the oldest (lex-tiebreak on id). Returns survivors in input order. Deterministic.
   */
  private List<TableOperationsRow> cancelDuplicates(List<TableOperationsRow> pendingRows) {
    Map<String, List<TableOperationsRow>> byTableUuid =
        pendingRows.stream().collect(Collectors.groupingBy(TableOperationsRow::getTableUuid));

    List<String> duplicateIds =
        byTableUuid.values().stream()
            .filter(rows -> rows.size() > 1)
            .flatMap(
                rows ->
                    rows.stream()
                        .sorted(
                            Comparator.comparing(TableOperationsRow::getCreatedAt)
                                .thenComparing(TableOperationsRow::getId))
                        .skip(1))
            .map(TableOperationsRow::getId)
            .collect(Collectors.toList());

    if (duplicateIds.isEmpty()) {
      return pendingRows;
    }

    int cancelled = operationsRepo.cancel(duplicateIds);
    log.warn("Cancelled {} duplicate PENDING rows", cancelled);

    Set<String> cancelledIds = Set.copyOf(duplicateIds);
    return pendingRows.stream()
        .filter(r -> !cancelledIds.contains(r.getId()))
        .collect(Collectors.toList());
  }

  private Map<String, TableStatsDto> loadStatsByUuid(List<TableOperationDto> ops) {
    Set<String> uuids =
        ops.stream().map(TableOperationDto::getTableUuid).collect(Collectors.toSet());
    return statsRepo.findAllById(uuids).stream()
        .collect(Collectors.toMap(TableStatsRow::getTableUuid, TableStatsDto::fromRow));
  }

  private List<BinItem> projectToItems(
      List<TableOperationDto> pending,
      Map<String, TableStatsDto> statsByUuid,
      BinItem prototype,
      OperationTypeDto type) {
    List<BinItem> items =
        pending.stream()
            .filter(op -> statsByUuid.containsKey(op.getTableUuid()))
            .map(op -> prototype.withOpAndStats(op, statsByUuid.get(op.getTableUuid())))
            .collect(Collectors.toList());
    int skipped = pending.size() - items.size();
    if (skipped > 0) {
      log.warn("Skipped {} {} operations with no table_stats row", skipped, type);
    }
    return items;
  }

  /**
   * Claim the bin's operations, narrow to the rows actually owned, launch one batched Spark job for
   * the claimed subset, and mark SCHEDULED — or revert to PENDING if launch failed.
   */
  @Transactional
  void scheduleBin(Bin bin) {
    List<BinItem> items = bin.getItems();
    OperationTypeDto type = bin.getOperationType();
    List<String> ids = items.stream().map(BinItem::getOperationId).collect(Collectors.toList());

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
        items.stream()
            .filter(item -> claimedSet.contains(item.getOperationId()))
            .collect(Collectors.toList());
    List<String> tableNames =
        claimedItems.stream().map(BinItem::getFullyQualifiedTableName).collect(Collectors.toList());
    List<String> operationIds =
        claimedItems.stream().map(BinItem::getOperationId).collect(Collectors.toList());

    String jobTypeName = type.name();
    String jobName = "batched-" + jobTypeName.toLowerCase() + "-" + claimedAt.toEpochMilli();
    Optional<String> jobId =
        jobsClient.launch(jobName, jobTypeName, tableNames, operationIds, resultsEndpoint);

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
