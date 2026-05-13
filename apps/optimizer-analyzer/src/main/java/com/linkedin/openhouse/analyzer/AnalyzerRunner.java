package com.linkedin.openhouse.analyzer;

import com.linkedin.openhouse.analyzer.model.OperationType;
import com.linkedin.openhouse.analyzer.model.Table;
import com.linkedin.openhouse.analyzer.model.TableOperation;
import com.linkedin.openhouse.optimizer.entity.TableOperationHistoryRow;
import com.linkedin.openhouse.optimizer.entity.TableOperationRow;
import com.linkedin.openhouse.optimizer.repository.TableOperationHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/*
 * Performance roadmap — read before scaling this runner past ~100k tables.
 *
 * Current shape: each pass loads all table_stats, all active table_operations
 * per op type, and the latest history row per (table_uuid, op_type) per op
 * type, then iterates (table × analyzer) in memory. Heap is O(tables) and
 * CPU is O(tables × op_types) per pass. This works at ~100k tables / daily
 * cadence with ~8 GB heap. It does NOT work at 1M tables / 15-min cadence:
 * heap peaks past 10 GB during ops loading, the history correlated subquery
 * does not complete without index (d) below, and per-pass wall time exceeds
 * the cadence window.
 *
 * The optimizations below are roughly ordered by when they matter. Items
 * (a)–(d) are hard prerequisites for scaling past ~100k tables; the rest
 * unlock further headroom. Items already landed are noted inline.
 *
 * Schema / data-model prerequisites:
 *   (a) Denormalized per-operation enablement on table_stats (or a sibling
 *       table), kept current by commit hooks on table-property changes. Each
 *       op type has its own enable flag — this mirrors the existing
 *       enable/disable pattern for non-optimizer maintenance operations on
 *       table properties (e.g. maintenance.optimizer.ofd.enabled). Lets the
 *       analyzer filter opted-in (table, op_type) pairs at index level
 *       instead of parsing the tableProperties JSON for every row. 10–100×
 *       data reduction in early rollout where opt-in is selective per op.
 *   (b) Index on table_operations(table_uuid, operation_type, created_at) —
 *       drives the cooldown anti-join in (e)/(f). Lands with that query.
 *   (c) Index supporting per-op opt-in lookup on the denormalized structure
 *       from (a). Shape depends on (a)'s representation. Lands with (e)/(f).
 *   (d) Index on table_operations_history(operation_type, table_uuid,
 *       completed_at) — makes findLatestPerTable O(N log N) instead of
 *       O(N²). Without this the query does not complete at 1M-row scale.
 *       LANDED: idx_toph_optype_uuid_completed.
 *
 * Query shape:
 *   (e) findCandidatesByDatabase(db, opType, cooldown) repo method that
 *       pushes both the per-op opt-in filter (from (a)) and the cooldown
 *       predicate to SQL via NOT EXISTS. In-cooldown and opted-out tables
 *       produce zero rows; their op rows are never materialized in the
 *       application. Single biggest win on data transfer and heap.
 *   (f) Combined-op variant of (e): one query per db returns candidates
 *       across all op types in one shot. Cuts read QPS ~10× at the same
 *       data volume (20k queries/pass vs 200k).
 *   (g) Per-database iteration replacing the current global scan. Bounds
 *       working set to one db (1–10k tables) per pass instead of the full
 *       table count.
 *   (h) Replace the findLatestPerTable consumer with the (e)/(f) anti-join,
 *       encoding cadence policy (success-retry-interval, failure-retry-
 *       interval, scheduled-timeout) in SQL WHERE clauses. If this subsumes
 *       all cadence reads, historyRepo on this class becomes dead and can
 *       be removed; if some strategies still need Java-side cadence on
 *       history rows, keep it.
 *
 * Projection / IO:
 *   (i) Drop tableProperties from the stats query projection once (a) lands.
 *       The TEXT column carries multi-KB JSON per row; at 1M rows that's
 *       gigabytes of wire transfer per pass for data nobody reads.
 *   (j) High-volume repo reads must page or stream — Stream<Entity> or
 *       paginated cursor, not List<Entity>. JPA materializes the full list
 *       before stream-collect, which is the dominant heap-spike source.
 *       Treat this as a baseline requirement, not an optional optimization.
 *   (k) Batch PENDING INSERTs via saveAll(500–1000) and set
 *       hibernate.jdbc.batch_size. Per-row save() dominates the write phase
 *       at any meaningful candidate volume.
 *
 * Runtime / deployment:
 *   (l) Rate-limit queries per analyzer instance to bound MySQL load.
 *       The runner should pace its per-db iteration across the cadence
 *       window rather than racing — converts a 2k-QPS spike at the start
 *       of each pass into a flat ~20 QPS sustained.
 *   (m) Conditional, only if single-instance runtime exceeds the cadence
 *       window after (a)–(l): shard databases by hash key and run N
 *       analyzer instances in parallel. Concurrency is then controlled by
 *       deployment count, not in-process worker pools.
 */
/**
 * Core analysis loop. Loads {@code table_stats} rows and evaluates each table against every
 * registered {@link OperationAnalyzer} in a single pass.
 *
 * <p>Both sides of the join — current operations and latest history per (table, type) — are loaded
 * into maps once per run before the table loop. This is correct at small scale (≤~100k tables) but
 * breaks past that; see the performance roadmap block comment above for the queued optimizations
 * and their triggering thresholds.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AnalyzerRunner {

  private final List<OperationAnalyzer> analyzers;
  private final TableStatsRepository statsRepo;
  private final TableOperationsRepository operationsRepo;
  private final TableOperationHistoryRepository historyRepo;

  /** Run the full analysis loop once with no filters. */
  public void analyze() {
    analyze(null, null, null, null);
  }

  /**
   * Run the analysis loop, optionally scoped to a specific operation type, database, table name, or
   * table UUID. Pass {@code null} for any parameter to skip that filter.
   */
  public void analyze(
      OperationType operationType, String databaseName, String tableName, String tableUuid) {

    List<OperationAnalyzer> activeAnalyzers =
        operationType == null
            ? analyzers
            : analyzers.stream()
                .filter(a -> a.getOperationType() == operationType)
                .collect(Collectors.toList());

    // Pre-load the small sides of the joins — one query per analyzer type.
    // TODO: Move to a query builder (Criteria API or jOOQ) as filter count grows.
    Map<OperationType, Map<String, TableOperation>> opsByType =
        activeAnalyzers.stream()
            .collect(
                Collectors.toMap(
                    OperationAnalyzer::getOperationType,
                    a ->
                        operationsRepo
                            .find(
                                a.getOperationType().name(),
                                null,
                                tableUuid,
                                databaseName,
                                tableName)
                            .stream()
                            .filter(e -> e.getTableUuid() != null)
                            .collect(
                                Collectors.toMap(
                                    TableOperationRow::getTableUuid,
                                    TableOperation::from,
                                    TableOperation::mostRecent))));

    // Latest history row per (table_uuid, operation_type), one query per analyzer. The repo query
    // may return tied rows for the same key on identical completed_at; dedupe in memory.
    Map<OperationType, Map<String, TableOperationHistoryRow>> latestHistoryByType =
        activeAnalyzers.stream()
            .collect(
                Collectors.toMap(
                    OperationAnalyzer::getOperationType,
                    a ->
                        historyRepo.findLatestPerTable(a.getOperationType().name()).stream()
                            .filter(r -> r.getTableUuid() != null)
                            .collect(
                                Collectors.toMap(
                                    TableOperationHistoryRow::getTableUuid,
                                    r -> r,
                                    AnalyzerRunner::moreRecentHistory))));

    List<Table> tables =
        statsRepo.find(databaseName, tableName, tableUuid).stream()
            .filter(row -> row.getTableUuid() != null)
            .map(Table::from)
            .collect(Collectors.toList());
    log.info("Found {} tables in optimizer table_stats", tables.size());

    tables.forEach(
        table ->
            activeAnalyzers.forEach(
                analyzer -> {
                  if (!analyzer.isEnabled(table)) {
                    return;
                  }

                  Optional<TableOperation> currentOp =
                      Optional.ofNullable(
                          opsByType.get(analyzer.getOperationType()).get(table.getTableUuid()));
                  Optional<TableOperationHistoryRow> latestHistory =
                      Optional.ofNullable(
                          latestHistoryByType
                              .get(analyzer.getOperationType())
                              .get(table.getTableUuid()));

                  if (analyzer.shouldSchedule(table, currentOp, latestHistory)) {
                    TableOperation op = TableOperation.pending(table, analyzer.getOperationType());
                    operationsRepo.save(op.toRow());
                    log.info(
                        "Created PENDING {} operation for table {}.{}",
                        analyzer.getOperationType(),
                        table.getDatabaseName(),
                        table.getTableId());
                  }
                }));

    log.info("Analysis complete");
  }

  private static TableOperationHistoryRow moreRecentHistory(
      TableOperationHistoryRow a, TableOperationHistoryRow b) {
    Comparator<TableOperationHistoryRow> byCompletedAt =
        Comparator.comparing(r -> r.getCompletedAt() != null ? r.getCompletedAt() : Instant.EPOCH);
    return byCompletedAt.compare(a, b) >= 0 ? a : b;
  }
}
