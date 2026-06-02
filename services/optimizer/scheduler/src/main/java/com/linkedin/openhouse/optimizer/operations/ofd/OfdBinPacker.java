package com.linkedin.openhouse.optimizer.operations.ofd;

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
import com.linkedin.openhouse.optimizer.scheduler.binpack.FirstFitDecreasingBinPacker;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

/**
 * Per-cycle OFD orchestrator. Loads PENDING OFD operations, deduplicates duplicates per cycle,
 * joins each to its stats row, projects into {@link OfdBinItem}, asks {@link
 * FirstFitDecreasingBinPacker} to group them, and returns each grouping wrapped in an {@link
 * OfdBin} that knows how to schedule itself.
 */
@Slf4j
@Component
public class OfdBinPacker implements BinPacker {

  private final FirstFitDecreasingBinPacker ffd;
  private final TableOperationsRepository operationsRepo;
  private final TableStatsRepository statsRepo;
  private final JobsServiceClient jobsClient;
  private final String resultsEndpoint;

  @Autowired
  public OfdBinPacker(
      @Value("${optimizer.scheduler.ofd.max-files-per-bin}") long maxFilesPerBin,
      @Value("${optimizer.scheduler.ofd.max-tables-per-bin}") int maxTablesPerBin,
      TableOperationsRepository operationsRepo,
      TableStatsRepository statsRepo,
      JobsServiceClient jobsClient,
      @Value("${optimizer.scheduler.results-endpoint}") String resultsEndpoint) {
    this.ffd =
        FirstFitDecreasingBinPacker.builder()
            .maxWeightPerBin(maxFilesPerBin)
            .maxItemsPerBin(maxTablesPerBin)
            .build();
    this.operationsRepo = operationsRepo;
    this.statsRepo = statsRepo;
    this.jobsClient = jobsClient;
    this.resultsEndpoint = resultsEndpoint;
  }

  @Override
  public OperationTypeDto getOperationType() {
    return OperationTypeDto.ORPHAN_FILES_DELETION;
  }

  @Override
  public List<Bin> prepare(Optional<String> databaseName, Optional<String> tableName) {
    // Unpaged: a single-page truncation would silently drop work past page 0 (next cycle would
    // re-load the same first page in MySQL row order, leaving the tail unscheduled until the
    // ordering shifts). Correctness here requires the full PENDING set in one cycle; the working
    // set is bounded by count(PENDING for OFD).
    List<TableOperationsRow> pendingRows =
        operationsRepo.find(
            Optional.of(OperationTypeDto.ORPHAN_FILES_DELETION.toDb()),
            Optional.of(OperationStatus.PENDING),
            Optional.empty(),
            databaseName,
            tableName,
            Optional.empty(),
            Optional.empty(),
            Pageable.unpaged());
    if (pendingRows.isEmpty()) {
      log.info("No PENDING OFD operations; nothing to prepare");
      return List.of();
    }

    // Deduplicate before claiming: if multiple PENDING rows exist for the same tableUuid, keep
    // the oldest (lex-tiebreak on id) and cancel the rest. Per-cycle, not per-bin.
    List<TableOperationsRow> survivors = cancelDuplicates(pendingRows);
    if (survivors.isEmpty()) {
      return List.of();
    }

    List<TableOperationDto> pending =
        survivors.stream().map(TableOperationDto::fromRow).collect(Collectors.toList());

    // Fetch fresh stats this cycle (one batched query) rather than denormalizing onto
    // TableOperationDto. Smaller op rows, fresher cost data.
    Set<String> uuids =
        pending.stream().map(TableOperationDto::getTableUuid).collect(Collectors.toSet());
    Map<String, TableStatsDto> statsByUuid =
        statsRepo.findAllById(uuids).stream()
            .collect(Collectors.toMap(TableStatsRow::getTableUuid, TableStatsDto::fromRow));

    // Filter at the boundary so every projection is built from a known-non-null stats row. A
    // table without a stats row gets skipped this cycle and reconsidered after stats land.
    List<TableOperationDto> withStats =
        pending.stream()
            .filter(op -> statsByUuid.containsKey(op.getTableUuid()))
            .collect(Collectors.toList());
    if (withStats.size() < pending.size()) {
      log.warn(
          "Skipped {} OFD operations with no table_stats row", pending.size() - withStats.size());
    }
    if (withStats.isEmpty()) {
      return List.of();
    }

    List<BinItem> items =
        withStats.stream()
            .<BinItem>map(op -> OfdBinItem.from(op, statsByUuid.get(op.getTableUuid())))
            .collect(Collectors.toList());

    List<List<BinItem>> groupings = ffd.pack(items);
    log.info("Prepared {} PENDING OFD operations into {} bins", items.size(), groupings.size());

    return groupings.stream().map(this::toOfdBin).collect(Collectors.toList());
  }

  private Bin toOfdBin(List<BinItem> grouping) {
    List<OfdBinItem> ofdItems =
        grouping.stream().map(OfdBinItem.class::cast).collect(Collectors.toList());
    return new OfdBin(ofdItems, operationsRepo, jobsClient, resultsEndpoint);
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
}
