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
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * For one operation type per call, reads PENDING rows, looks up per-table stats, projects each into
 * a {@link BinItem}, dispatches to the registered {@link BinPacker}, and submits one Spark job per
 * returned {@link Bin}. The {@link com.linkedin.openhouse.optimizer.scheduler.SchedulerApplication}
 * 's CommandLineRunner loops over the registered packers and invokes {@code schedule(opType)} for
 * each.
 *
 * <p>The runner owns all optimizer-specific orchestration — claim CAS, status transitions, and the
 * actual {@link JobsServiceClient#launch} call. The bin packer is a pure utility over a flat list
 * of {@link BinItem}s, deliberately decoupled from operation types and JPA rows so the same packer
 * can be shared with the existing {@code JobsScheduler} flow.
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
      @Value("${optimizer.scheduler.results-endpoint}") String resultsEndpoint) {
    this.operationsRepo = operationsRepo;
    this.statsRepo = statsRepo;
    this.jobsClient = jobsClient;
    this.binPackers = binPackers;
    this.resultsEndpoint = resultsEndpoint;
  }

  /** Schedule all PENDING operations of the given type across all databases. */
  @Transactional
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

    // Unpaged: a single-page truncation would silently drop work past page 0 (next cycle would
    // re-load the same first page in MySQL row order, leaving the tail unscheduled until the
    // ordering shifts). Correctness here requires the full PENDING set in one cycle; the working
    // set is bounded by count(PENDING for this op type).
    List<TableOperationsRow> pendingRows =
        operationsRepo.find(
            Optional.of(operationType.toDb()),
            Optional.of(OperationStatus.PENDING),
            Optional.empty(),
            databaseName,
            tableName,
            Optional.empty(),
            Optional.empty(),
            Pageable.unpaged());
    if (pendingRows.isEmpty()) {
      log.info("No PENDING operations of type {}; nothing to schedule", operationType);
      return;
    }

    // Deduplicate before claiming: if multiple PENDING rows exist for the same tableUuid, keep
    // the oldest (lex-tiebreak on id) and cancel the rest. Per-cycle, not per-bin — running this
    // inside the bin loop nuked rows belonging to other bins of the same cycle.
    List<TableOperationsRow> survivors = cancelDuplicates(pendingRows);
    if (survivors.isEmpty()) {
      return;
    }

    List<TableOperationDto> pending =
        survivors.stream().map(TableOperationDto::fromRow).collect(Collectors.toList());

    // Tradeoff: we fetch fresh table_stats per scheduling cycle (one batched query) rather than
    // denormalizing the relevant fields onto TableOperationDto. The denormalized alternative would
    // remove the per-cycle lookup but widen the TableOperationDto row and serve staler data; the
    // current shape favors smaller operations + freshness over fewer queries.
    Set<String> uuids =
        pending.stream().map(TableOperationDto::getTableUuid).collect(Collectors.toSet());
    Map<String, TableStatsDto> statsByUuid =
        statsRepo.findAllById(uuids).stream()
            .collect(Collectors.toMap(TableStatsRow::getTableUuid, TableStatsDto::fromRow));

    // Filter at the boundary so every BinItem is built from a known-non-null stats row. A table
    // without a stats row gets skipped this cycle and reconsidered after stats land.
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

    List<BinItem> items =
        withStats.stream()
            .map(op -> toBinItem(op, statsByUuid.get(op.getTableUuid())))
            .collect(Collectors.toList());

    List<Bin> bins = packer.pack(items);
    log.info(
        "Packed {} PENDING {} operations into {} bins", items.size(), operationType, bins.size());

    bins.forEach(bin -> submitBin(operationType, bin));
  }

  /**
   * Project an (operation, stats) pair into the packer's input row. Weight is current file count
   * (the packing dimension OFD cares about); sizeBytes is the on-disk footprint when stats expose
   * it, else 0.
   */
  private static BinItem toBinItem(TableOperationDto op, TableStatsDto stats) {
    long weight = 0L;
    long sizeBytes = 0L;
    if (stats != null && stats.getSnapshot() != null) {
      Long files = stats.getSnapshot().getNumCurrentFiles();
      if (files != null) {
        weight = files;
      }
      Long bytes = stats.getSnapshot().getTableSizeBytes();
      if (bytes != null) {
        sizeBytes = bytes;
      }
    }
    return BinItem.builder()
        .fqtn(op.getDatabaseName() + "." + op.getTableName())
        .operationId(op.getId())
        .tableUuid(op.getTableUuid())
        .databaseName(op.getDatabaseName())
        .tableName(op.getTableName())
        .weight(weight)
        .sizeBytes(sizeBytes)
        .build();
  }

  /**
   * Group {@code pendingRows} by {@code tableUuid}; for any group with more than one row, cancel
   * all but the oldest (lex-tiebreak on id). Returns the survivors in input order. Deterministic.
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

  /**
   * Claim the bin, narrow to the rows actually claimed, launch the batched Spark job for the
   * claimed subset, and mark them SCHEDULED — or revert to PENDING if launch failed.
   */
  private void submitBin(OperationTypeDto operationType, Bin bin) {
    List<String> ids =
        bin.items().stream().map(BinItem::getOperationId).collect(Collectors.toList());

    // Claim in one batched UPDATE: PENDING → SCHEDULING. Aggregate row count alone doesn't tell us
    // *which* rows we own — re-query for SCHEDULING rows tagged with our scheduledAt watermark.
    // Anything not in that subset belongs to another instance or was canceled, and must not be
    // submitted or marked SCHEDULED.
    Instant claimedAt = Instant.now();
    operationsRepo.updateBatch(
        ids,
        OperationStatus.PENDING,
        OperationStatus.SCHEDULING,
        Optional.of(claimedAt),
        Optional.empty());
    // Unpaged: the result set is bounded by ids.size() (the bin we just claimed).
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

    // Narrow the bin's items to the rows we actually own before extracting Spark-args.
    Set<String> claimedSet = new HashSet<>(claimedIds);
    List<BinItem> claimedItems =
        bin.items().stream()
            .filter(item -> claimedSet.contains(item.getOperationId()))
            .collect(Collectors.toList());
    List<String> tableNames =
        claimedItems.stream().map(BinItem::getFqtn).collect(Collectors.toList());
    List<String> operationIds =
        claimedItems.stream().map(BinItem::getOperationId).collect(Collectors.toList());

    String jobName =
        "batched-" + operationType.name().toLowerCase() + "-" + claimedAt.toEpochMilli();
    Optional<String> jobId =
        jobsClient.launch(jobName, operationType.name(), tableNames, operationIds, resultsEndpoint);

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
