package com.linkedin.openhouse.optimizer.analyzer;

import com.linkedin.openhouse.optimizer.db.AnalyzerRunStateRow;
import com.linkedin.openhouse.optimizer.model.ChangedTableDto;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.repository.AnalyzerRunStateRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

/**
 * Core analysis loop. For one operation type per call, evaluates each candidate table against the
 * matching {@link OperationAnalyzer} and upserts PENDING operations for tables that need work.
 *
 * <p>Two scan modes:
 *
 * <ul>
 *   <li><b>Incremental</b> ({@link #analyzeIncremental}) — the normal cadence. A single join query
 *       ({@code TableStatsRepository.findChangedWithOpAndLatestHistory}) returns every table
 *       changed since the last pass ({@code updated_at >= watermark}) together with its current op
 *       and latest history — so all three tables are read for only the changed set, in one round
 *       trip, no per-table {@code IN} lists. The watermark is persisted per operation type in
 *       {@code analyzer_run_state}.
 *   <li><b>Full</b> ({@link #analyze}) — a periodic safety-net that scans every table, per database
 *       and in parallel. Needed because some triggers (e.g. the idle-table OFD sweep) fire on
 *       tables that have <i>not</i> committed and so never appear in an incremental scan.
 * </ul>
 *
 * <p>Work is fanned out across a bounded thread pool (size {@code analyzer.db-parallelism}); each
 * unit is independent and a failure in one is isolated and logged, never aborting the rest.
 *
 * <p><b>Connection-pool sizing.</b> Each in-flight task holds at most one DB connection, so the
 * datasource pool (e.g. {@code spring.datasource.hikari.maximum-pool-size}) must be at least {@code
 * analyzer.db-parallelism} (plus headroom) or workers block on connection acquisition.
 *
 * <p><b>Transactions.</b> Per-unit work intentionally runs <i>without</i> a wrapping transaction:
 * each {@code operationsRepo.save(...)} commits on its own (Spring Data default). This lets a
 * single bad table be caught and skipped without poisoning the rest. Do NOT wrap the evaluate loop
 * in {@code @Transactional} — a caught save failure would mark the shared transaction
 * rollback-only.
 */
@Slf4j
@Component
public class AnalyzerRunner {

  private final List<OperationAnalyzer> analyzers;
  private final TableStatsRepository statsRepo;
  private final TableOperationsRepository operationsRepo;
  private final TableOperationsHistoryRepository historyRepo;
  private final AnalyzerRunStateRepository runStateRepo;
  private final ExecutorService dbExecutor;
  private final int dbParallelism;

  public AnalyzerRunner(
      List<OperationAnalyzer> analyzers,
      TableStatsRepository statsRepo,
      TableOperationsRepository operationsRepo,
      TableOperationsHistoryRepository historyRepo,
      AnalyzerRunStateRepository runStateRepo,
      @Value("${analyzer.db-parallelism:8}") int dbParallelism) {
    this.analyzers = analyzers;
    this.statsRepo = statsRepo;
    this.operationsRepo = operationsRepo;
    this.historyRepo = historyRepo;
    this.runStateRepo = runStateRepo;
    this.dbParallelism = Math.max(1, dbParallelism);
    this.dbExecutor =
        Executors.newFixedThreadPool(this.dbParallelism, daemonThreadFactory("analyzer-db"));
  }

  // ---------------------------------------------------------------------------
  // Incremental scan (normal cadence)
  // ---------------------------------------------------------------------------

  /**
   * Evaluate only the tables written since the last pass for {@code operationType} (joined to their
   * current op and latest history), then advance the persisted watermark. Changed tables are
   * grouped by database and evaluated in parallel.
   *
   * <p>The watermark is advanced to this run's start time even when nothing changed, and regardless
   * of per-table save failures — the periodic {@link #analyze full scan} reconciles anything a
   * transient failure skipped, so the incremental path never needs to "hold back" the watermark.
   */
  public void analyzeIncremental(OperationTypeDto operationType) {
    OperationAnalyzer analyzer = resolveAnalyzer(operationType);
    Instant watermark =
        runStateRepo
            .findById(operationType.name())
            .map(AnalyzerRunStateRow::getWatermark)
            .orElse(Instant.EPOCH);
    Instant runStart = Instant.now();

    List<ChangedTableDto> changed =
        statsRepo
            .findChangedWithOpAndLatestHistory(operationType.toDb(), watermark, Pageable.unpaged())
            .stream()
            .map(ChangedTableDto::fromJoinRow)
            .filter(c -> c.getTable() != null && c.getTable().getTableUuid() != null)
            .collect(Collectors.toList());
    log.info(
        "Incremental analyze {}: {} changed table(s) since {}",
        operationType,
        changed.size(),
        watermark);

    if (!changed.isEmpty()) {
      EvaluationResult result = evaluateChangedInParallel(analyzer, changed);
      log.info(
          "Incremental analyze {} finished: created {} PENDING op(s) ({} failed, {} skipped) from {}"
              + " changed table(s)",
          operationType,
          result.getCreated(),
          result.getFailed(),
          result.getSkipped(),
          changed.size());
    }

    runStateRepo.save(
        AnalyzerRunStateRow.builder()
            .operationType(operationType.name())
            .watermark(runStart)
            .build());
  }

  /**
   * Group the changed rows by database and evaluate each database in parallel on the bounded pool.
   * A failure in one database is isolated and logged, never aborting the others.
   */
  private EvaluationResult evaluateChangedInParallel(
      OperationAnalyzer analyzer, List<ChangedTableDto> changed) {
    Map<String, List<ChangedTableDto>> byDatabase =
        changed.stream().collect(Collectors.groupingBy(c -> c.getTable().getDatabaseName()));
    List<CompletableFuture<EvaluationResult>> futures =
        byDatabase.entrySet().stream()
            .map(
                e ->
                    CompletableFuture.supplyAsync(
                        () -> evaluateChangedDatabaseSafely(analyzer, e.getKey(), e.getValue()),
                        dbExecutor))
            .collect(Collectors.toList());
    EvaluationResult result = EvaluationResult.empty();
    for (CompletableFuture<EvaluationResult> f : futures) {
      result = result.combine(f.join()); // each task isolates its own failures and never throws
    }
    return result;
  }

  private EvaluationResult evaluateChangedDatabaseSafely(
      OperationAnalyzer analyzer, String databaseName, List<ChangedTableDto> tables) {
    try {
      return evaluateChanged(analyzer, tables);
    } catch (Exception e) {
      log.error(
          "Incremental analysis failed for database {} (operation {}): {}",
          databaseName,
          analyzer.getOperationType(),
          e.toString(),
          e);
      return EvaluationResult.empty();
    }
  }

  private EvaluationResult evaluateChanged(
      OperationAnalyzer analyzer, List<ChangedTableDto> changed) {
    EvaluationResult result = EvaluationResult.empty();
    for (ChangedTableDto row : changed) {
      TableDto table = row.getTable();
      if (!analyzer.isEnabled(table)
          || !analyzer.shouldSchedule(table, row.currentOp(), row.latestHistory())) {
        result = result.combine(EvaluationResult.skipped());
        continue;
      }
      result = result.combine(createPending(analyzer, table));
    }
    return result;
  }

  // ---------------------------------------------------------------------------
  // Full scan (periodic safety-net)
  // ---------------------------------------------------------------------------

  /**
   * Run a full analysis for {@code operationType} across all databases, with no filters. Does not
   * touch the incremental watermark (a full scan is a superset of any incremental one).
   */
  public void analyze(OperationTypeDto operationType) {
    analyze(operationType, Optional.empty(), Optional.empty(), Optional.empty());
  }

  /**
   * Run a full analysis for the given operation type, optionally scoped to a single database, table
   * name, or table UUID. Databases are fanned out across the bounded pool; the call blocks until
   * every database has been analyzed.
   */
  public void analyze(
      OperationTypeDto operationType,
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid) {
    OperationAnalyzer analyzer = resolveAnalyzer(operationType);
    List<String> dbs = databaseName.map(List::of).orElseGet(statsRepo::findDistinctDatabaseNames);
    log.info(
        "Full analyze {} across {} database(s) with parallelism {}",
        operationType,
        dbs.size(),
        dbParallelism);

    CompletableFuture<?>[] futures =
        dbs.stream()
            .map(
                db ->
                    CompletableFuture.runAsync(
                        () -> analyzeDatabaseSafely(analyzer, db, tableName, tableUuid),
                        dbExecutor))
            .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(futures).join();
    log.info("Full analysis complete for {}", operationType);
  }

  private void analyzeDatabaseSafely(
      OperationAnalyzer analyzer,
      String databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid) {
    try {
      analyzeDatabase(analyzer, databaseName, tableName, tableUuid);
    } catch (Exception e) {
      // One database failing (including a failed read query) must not abort the others; the next
      // pass retries it.
      log.error(
          "Analysis failed for database {} (operation {}): {}",
          databaseName,
          analyzer.getOperationType(),
          e.toString(),
          e);
    }
  }

  void analyzeDatabase(
      OperationAnalyzer analyzer,
      String databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid) {
    Map<String, TableOperationDto> currentOps =
        loadCurrentOps(analyzer, Optional.of(databaseName), tableName, tableUuid);
    Map<String, TableOperationsHistoryDto> latestHistory = loadLatestHistory(analyzer);
    List<TableDto> tables =
        statsRepo.find(Optional.of(databaseName), tableName, tableUuid, Pageable.unpaged()).stream()
            .filter(row -> row.getTableUuid() != null)
            .map(TableDto::fromRow)
            .collect(Collectors.toList());

    EvaluationResult result = evaluateTables(analyzer, tables, currentOps, latestHistory);
    log.info(
        "Finished analyzing Database {}: created {} PENDING {} operation(s) ({} failed, {} skipped)",
        databaseName,
        result.getCreated(),
        analyzer.getOperationType(),
        result.getFailed(),
        result.getSkipped());
  }

  // ---------------------------------------------------------------------------
  // Shared helpers
  // ---------------------------------------------------------------------------

  private OperationAnalyzer resolveAnalyzer(OperationTypeDto operationType) {
    return analyzers.stream()
        .filter(a -> a.getOperationType() == operationType)
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "No analyzer registered for operation type " + operationType));
  }

  /** Full-scan: all active ops for a database (no UUID filter). */
  private Map<String, TableOperationDto> loadCurrentOps(
      OperationAnalyzer analyzer,
      Optional<String> databaseName,
      Optional<String> tableName,
      Optional<String> tableUuid) {
    return operationsRepo
        .find(
            Optional.of(analyzer.getOperationType().toDb()),
            Optional.empty(),
            tableUuid,
            databaseName,
            tableName,
            Optional.empty(),
            Optional.empty(),
            Pageable.unpaged())
        .stream()
        .filter(e -> e.getTableUuid() != null)
        .map(TableOperationDto::fromRow)
        .collect(
            Collectors.toMap(
                TableOperationDto::getTableUuid, op -> op, TableOperationDto::mostRecent));
  }

  /** Full-scan: latest history per table for the operation type (whole fleet). */
  private Map<String, TableOperationsHistoryDto> loadLatestHistory(OperationAnalyzer analyzer) {
    return historyRepo.findLatest(analyzer.getOperationType().toDb(), Pageable.unpaged()).stream()
        .filter(r -> r.getTableUuid() != null)
        .map(TableOperationsHistoryDto::fromRow)
        .collect(
            Collectors.toMap(
                TableOperationsHistoryDto::getTableUuid, h -> h, TableOperationsHistoryDto::after));
  }

  /**
   * For each table: skip if not opted-in or if the analyzer declines; otherwise persist a PENDING
   * operation.
   */
  private EvaluationResult evaluateTables(
      OperationAnalyzer analyzer,
      List<TableDto> tables,
      Map<String, TableOperationDto> currentOps,
      Map<String, TableOperationsHistoryDto> latestHistory) {
    EvaluationResult result = EvaluationResult.empty();
    for (TableDto table : tables) {
      if (!analyzer.isEnabled(table)) {
        result = result.combine(EvaluationResult.skipped());
        continue;
      }
      Optional<TableOperationDto> currentOp =
          Optional.ofNullable(currentOps.get(table.getTableUuid()));
      Optional<TableOperationsHistoryDto> entry =
          Optional.ofNullable(latestHistory.get(table.getTableUuid()));
      if (!analyzer.shouldSchedule(table, currentOp, entry)) {
        result = result.combine(EvaluationResult.skipped());
        continue;
      }
      result = result.combine(createPending(analyzer, table));
    }
    return result;
  }

  /**
   * Persist one PENDING operation, isolating failures. A single bad table is caught and skipped (no
   * wrapping transaction, so it does not affect the other tables' saves).
   */
  private EvaluationResult createPending(OperationAnalyzer analyzer, TableDto table) {
    try {
      operationsRepo.save(TableOperationDto.pending(table, analyzer.getOperationType()).toRow());
      log.info(
          "Created PENDING {} operation for table {}.{}",
          analyzer.getOperationType(),
          table.getDatabaseName(),
          table.getTableId());
      return EvaluationResult.created();
    } catch (Exception e) {
      log.error(
          "Failed to create PENDING {} operation for table {}.{}: {}",
          analyzer.getOperationType(),
          table.getDatabaseName(),
          table.getTableId(),
          e.toString(),
          e);
      return EvaluationResult.failed();
    }
  }

  @PreDestroy
  void shutdown() {
    dbExecutor.shutdown();
    try {
      if (!dbExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        dbExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      dbExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private static ThreadFactory daemonThreadFactory(String prefix) {
    AtomicInteger counter = new AtomicInteger();
    return runnable -> {
      Thread thread = new Thread(runnable, prefix + "-" + counter.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }

  /**
   * Immutable outcome of evaluating one or more candidate tables: how many PENDING ops were {@code
   * created}, how many saves {@code failed}, and how many tables were {@code skipped} (not opted-in
   * or the analyzer declined to schedule). Per-table and per-database results are accumulated with
   * {@link #combine}.
   */
  @Getter
  static final class EvaluationResult {
    private final int created;
    private final int failed;
    private final int skipped;

    private EvaluationResult(int created, int failed, int skipped) {
      this.created = created;
      this.failed = failed;
      this.skipped = skipped;
    }

    static EvaluationResult empty() {
      return new EvaluationResult(0, 0, 0);
    }

    static EvaluationResult created() {
      return new EvaluationResult(1, 0, 0);
    }

    static EvaluationResult failed() {
      return new EvaluationResult(0, 1, 0);
    }

    static EvaluationResult skipped() {
      return new EvaluationResult(0, 0, 1);
    }

    EvaluationResult combine(EvaluationResult other) {
      return new EvaluationResult(
          created + other.created, failed + other.failed, skipped + other.skipped);
    }
  }
}
