package com.linkedin.openhouse.optimizer.scheduler;

import static com.linkedin.openhouse.optimizer.scheduler.ExpiredTableDeletionScaleFixture.TableKind.FAILED_IN_BACKOFF;
import static com.linkedin.openhouse.optimizer.scheduler.ExpiredTableDeletionScaleFixture.TableKind.FAILED_RETRY_DUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.linkedin.openhouse.optimizer.analyzer.AnalyzeRequest;
import com.linkedin.openhouse.optimizer.analyzer.AnalyzerRunner;
import com.linkedin.openhouse.optimizer.analyzer.ExpiredTableDeletionAnalyzer;
import com.linkedin.openhouse.optimizer.binpack.FirstFitDecreasingBinPacker;
import com.linkedin.openhouse.optimizer.binpack.TotalFilesBinItem;
import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.TableOperationsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.repository.TableOperationsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import com.linkedin.openhouse.optimizer.scheduler.account.DummyAccountResolver;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;

/**
 * Runs the expired table deletion analyzer and scheduler over {@value
 * ExpiredTableDeletionScaleFixture#TABLE_COUNT} synthetic tables held in memory, so the measured
 * time and allocation belong to the Optimizer's own code rather than to a database. {@link
 * ExpiredTableDeletionScaleIntegrationTest} runs the same kinds of tables on MySQL.
 *
 * <p>The time limits are several times the observed run time so the test does not fail on a slow
 * build machine; they catch a change that makes a pass grow faster than linearly with the table
 * count.
 */
@Tag("scale")
class ExpiredTableDeletionScaleTest {

  private static final OperationTypeDto DELETION = OperationTypeDto.EXPIRED_TABLE_DELETION;
  private static final Duration PASS_TIME_LIMIT = Duration.ofSeconds(10);

  /**
   * Allocation allowed per table in one analyzer pass, including the in-memory repositories. The
   * observed figure is about 4 KB; a pass that copies its inputs for each table exceeds the limit.
   */
  private static final long ANALYZER_BYTES_PER_TABLE_LIMIT = 64 * 1024;

  private final InMemoryOptimizerDatabase database = new InMemoryOptimizerDatabase();
  private final List<List<String>> launchedJobTables = new ArrayList<>();

  @Test
  void analyzerAndScheduler_submitExactlyTheDueTables_withinBoundedTimeAndMemory() {
    ExpiredTableDeletionScaleFixture fixture =
        ExpiredTableDeletionScaleFixture.generate(Instant.now());
    database.load(fixture);
    AnalyzerRunner analyzer =
        new AnalyzerRunner(
            List.of(new ExpiredTableDeletionAnalyzer()),
            database.statsRepository(),
            database.operationsRepository(),
            database.historyRepository());
    SchedulerRunner scheduler =
        new SchedulerRunner(
                database.operationsRepository(),
                database.statsRepository(),
                jobsClient(),
                new DummyAccountResolver(),
                "http://optimizer/operations")
            .registerOperation(
                DELETION,
                FirstFitDecreasingBinPacker.<TotalFilesBinItem>builder()
                    .binItemSupplier(TotalFilesBinItem::new)
                    .maxItemsPerBin(1)
                    .build());

    long allocatedBeforeAnalysis = currentThreadAllocatedBytes();
    Duration analysisTime =
        timed(
            () ->
                analyzer.analyze(
                    AnalyzeRequest.builder().operationTypes(Set.of(DELETION)).build()));
    long analysisBytes = currentThreadAllocatedBytes() - allocatedBeforeAnalysis;
    System.out.printf(
        "Analyzer pass over %d tables: %d ms, %d MB allocated (%d bytes per table)%n",
        ExpiredTableDeletionScaleFixture.TABLE_COUNT,
        analysisTime.toMillis(),
        analysisBytes / (1024 * 1024),
        analysisBytes / ExpiredTableDeletionScaleFixture.TABLE_COUNT);

    assertThat(database.tablesWithNewPendingOperations())
        .isEqualTo(fixture.getTablesAnalyzerSchedules());
    assertThat(analysisTime).isLessThan(PASS_TIME_LIMIT);
    assertThat(analysisBytes)
        .isLessThan(ANALYZER_BYTES_PER_TABLE_LIMIT * ExpiredTableDeletionScaleFixture.TABLE_COUNT);
    // Bulk loads happen once per database, and only tables whose latest deletion failed pay for
    // the per-table failure streak reads.
    assertThat(database.tableStatsLoads).isEqualTo(ExpiredTableDeletionScaleFixture.DATABASE_COUNT);
    assertThat(database.recentHistoryReads)
        .isEqualTo(fixture.countOf(FAILED_IN_BACKOFF) + fixture.countOf(FAILED_RETRY_DUE));
    assertThat(database.recentSubmissionReads).isEqualTo(database.recentHistoryReads);

    Duration schedulingTime = timed(() -> scheduler.schedule(DELETION));
    System.out.printf(
        "Scheduler pass over %d pending deletions: %d ms%n",
        fixture.getTablesSchedulerSubmits().size(), schedulingTime.toMillis());

    assertThat(launchedJobTables).allSatisfy(tables -> assertThat(tables).hasSize(1));
    assertThat(launchedJobTables.stream().flatMap(List::stream).collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(fixture.getTablesSchedulerSubmits());
    assertThat(database.pendingOperationCount()).isZero();
    assertThat(schedulingTime).isLessThan(PASS_TIME_LIMIT);
  }

  private JobsServiceClient jobsClient() {
    JobsServiceClient client = mock(JobsServiceClient.class, withSettings().stubOnly());
    when(client.launch(anyString(), anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenAnswer(
            invocation -> {
              launchedJobTables.add(List.copyOf(invocation.<List<String>>getArgument(3)));
              return Optional.of("job-" + launchedJobTables.size());
            });
    return client;
  }

  private static Duration timed(Runnable pass) {
    long start = System.nanoTime();
    pass.run();
    return Duration.ofNanos(System.nanoTime() - start);
  }

  private static long currentThreadAllocatedBytes() {
    return ((com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean())
        .getThreadAllocatedBytes(Thread.currentThread().getId());
  }

  /**
   * The rows of the three optimizer tables, served through repository mocks that answer each query
   * the analyzer and scheduler issue the way the JPA query does. The mocks are stub-only, so
   * Mockito does not retain their invocations. Rows are indexed by table so per-table queries cost
   * what an indexed database lookup would.
   */
  private static final class InMemoryOptimizerDatabase {
    private final Map<String, TableStatsRow> statsByTableUuid = new HashMap<>();
    private final Map<String, TableOperationsRow> operationsById = new HashMap<>();
    private final Map<String, List<String>> operationIdsByTableUuid = new HashMap<>();
    private final Set<String> loadedOperationIds = new HashSet<>();
    private final Map<String, List<TableOperationsHistoryRow>> historyByTableUuid = new HashMap<>();
    private final TableStatsRepository statsRepository =
        mock(TableStatsRepository.class, withSettings().stubOnly());
    private final TableOperationsRepository operationsRepository =
        mock(TableOperationsRepository.class, withSettings().stubOnly());
    private final TableOperationsHistoryRepository historyRepository =
        mock(TableOperationsHistoryRepository.class, withSettings().stubOnly());
    private long tableStatsLoads;
    private long recentHistoryReads;
    private long recentSubmissionReads;

    InMemoryOptimizerDatabase() {
      when(statsRepository.findDistinctDatabaseNames())
          .thenAnswer(
              invocation ->
                  statsByTableUuid.values().stream()
                      .map(TableStatsRow::getDatabaseName)
                      .distinct()
                      .collect(Collectors.toList()));
      when(statsRepository.find(any(), any(), any(), any()))
          .thenAnswer(
              invocation ->
                  findTableStats(
                      invocation.getArgument(0),
                      invocation.getArgument(1),
                      invocation.getArgument(2)));
      when(statsRepository.findAllById(any()))
          .thenAnswer(
              invocation ->
                  StreamSupport.stream(
                          invocation.<Iterable<String>>getArgument(0).spliterator(), false)
                      .map(statsByTableUuid::get)
                      .filter(Objects::nonNull)
                      .collect(Collectors.toList()));
      when(operationsRepository.find(any(), any(), any(), any(), any(), any(), any(), any()))
          .thenAnswer(
              invocation ->
                  findOperations(
                      invocation.getArgument(0),
                      invocation.getArgument(1),
                      invocation.getArgument(2),
                      invocation.getArgument(3),
                      invocation.getArgument(4),
                      invocation.getArgument(5),
                      invocation.getArgument(6)));
      when(operationsRepository.save(any()))
          .thenAnswer(invocation -> saveOperation(invocation.getArgument(0)));
      when(operationsRepository.updateBatch(any(), any(), any(), any(), any()))
          .thenAnswer(
              invocation ->
                  updateOperations(
                      invocation.getArgument(0),
                      invocation.getArgument(1),
                      invocation.getArgument(2),
                      invocation.getArgument(3),
                      invocation.getArgument(4)));
      when(operationsRepository.findRecentSubmissions(any(), any(), any()))
          .thenAnswer(
              invocation ->
                  findRecentSubmissions(
                      invocation.getArgument(0),
                      invocation.getArgument(1),
                      invocation.getArgument(2)));
      when(historyRepository.findLatest(any(), any()))
          .thenAnswer(invocation -> findLatestHistory(invocation.getArgument(0)));
      when(historyRepository.findRecent(any(), any(), any()))
          .thenAnswer(
              invocation ->
                  findRecentHistory(
                      invocation.getArgument(0),
                      invocation.getArgument(1),
                      invocation.getArgument(2)));
    }

    void load(ExpiredTableDeletionScaleFixture fixture) {
      fixture.getOperations().forEach(row -> loadedOperationIds.add(row.getId()));
      fixture.getTableStats().forEach(row -> statsByTableUuid.put(row.getTableUuid(), row));
      fixture.getOperations().forEach(this::saveOperation);
      fixture
          .getHistory()
          .forEach(
              row ->
                  historyByTableUuid
                      .computeIfAbsent(row.getTableUuid(), uuid -> new ArrayList<>())
                      .add(row));
    }

    TableStatsRepository statsRepository() {
      return statsRepository;
    }

    TableOperationsRepository operationsRepository() {
      return operationsRepository;
    }

    TableOperationsHistoryRepository historyRepository() {
      return historyRepository;
    }

    /** Tables with a PENDING operation that the fixture did not load. */
    Set<String> tablesWithNewPendingOperations() {
      return operationsById.values().stream()
          .filter(row -> row.getStatus() == OperationStatus.PENDING)
          .filter(row -> !loadedOperationIds.contains(row.getId()))
          .map(ExpiredTableDeletionScaleFixture::fullyQualifiedName)
          .collect(Collectors.toSet());
    }

    long pendingOperationCount() {
      return operationsById.values().stream()
          .filter(row -> row.getStatus() == OperationStatus.PENDING)
          .count();
    }

    private List<TableStatsRow> findTableStats(
        Optional<String> databaseName, Optional<String> tableName, Optional<String> tableUuid) {
      tableStatsLoads++;
      return statsByTableUuid.values().stream()
          .filter(row -> matches(databaseName, row.getDatabaseName()))
          .filter(row -> matches(tableName, row.getTableName()))
          .filter(row -> matches(tableUuid, row.getTableUuid()))
          .collect(Collectors.toList());
    }

    private List<TableOperationsRow> findOperations(
        Optional<OperationType> operationType,
        Optional<OperationStatus> status,
        Optional<String> tableUuid,
        Optional<String> databaseName,
        Optional<String> tableName,
        Optional<Instant> scheduledAt,
        Optional<List<String>> ids) {
      Stream<TableOperationsRow> candidates =
          ids.map(idList -> idList.stream().map(operationsById::get).filter(Objects::nonNull))
              .orElseGet(() -> operationsById.values().stream());
      return candidates
          .filter(row -> matches(operationType, row.getOperationType()))
          .filter(row -> matches(status, row.getStatus()))
          .filter(row -> matches(tableUuid, row.getTableUuid()))
          .filter(row -> matches(databaseName, row.getDatabaseName()))
          .filter(row -> matches(tableName, row.getTableName()))
          .filter(row -> matches(scheduledAt, row.getScheduledAt()))
          .collect(Collectors.toList());
    }

    private TableOperationsRow saveOperation(TableOperationsRow row) {
      if (operationsById.put(row.getId(), row) == null) {
        operationIdsByTableUuid
            .computeIfAbsent(row.getTableUuid(), uuid -> new ArrayList<>())
            .add(row.getId());
      }
      return row;
    }

    private int updateOperations(
        List<String> ids,
        OperationStatus fromStatus,
        OperationStatus toStatus,
        Optional<Instant> scheduledAt,
        Optional<String> jobId) {
      List<TableOperationsRow> updated =
          ids.stream()
              .map(operationsById::get)
              .filter(Objects::nonNull)
              .filter(row -> row.getStatus() == fromStatus)
              .map(
                  row ->
                      row.toBuilder()
                          .status(toStatus)
                          .scheduledAt(scheduledAt.orElse(row.getScheduledAt()))
                          .jobId(jobId.orElse(row.getJobId()))
                          .build())
              .collect(Collectors.toList());
      updated.forEach(this::saveOperation);
      return updated.size();
    }

    private List<TableOperationsRow> findRecentSubmissions(
        String tableUuid, OperationType operationType, Pageable pageable) {
      recentSubmissionReads++;
      return operationIdsByTableUuid.getOrDefault(tableUuid, List.of()).stream()
          .map(operationsById::get)
          .filter(row -> row.getOperationType() == operationType)
          .filter(row -> row.getScheduledAt() != null)
          .sorted(Comparator.comparing(TableOperationsRow::getScheduledAt).reversed())
          .limit(pageable.getPageSize())
          .collect(Collectors.toList());
    }

    /** Like the JPA query, this ignores the database: every analyzed database reads all of it. */
    private List<TableOperationsHistoryRow> findLatestHistory(OperationType operationType) {
      return historyByTableUuid.values().stream()
          .flatMap(
              rows ->
                  rows.stream()
                      .filter(row -> row.getOperationType() == operationType)
                      .max(Comparator.comparing(TableOperationsHistoryRow::getCompletedAt))
                      .stream())
          .collect(Collectors.toList());
    }

    private List<TableOperationsHistoryRow> findRecentHistory(
        String tableUuid, OperationType operationType, Pageable pageable) {
      recentHistoryReads++;
      return historyByTableUuid.getOrDefault(tableUuid, List.of()).stream()
          .filter(row -> row.getOperationType() == operationType)
          .sorted(Comparator.comparing(TableOperationsHistoryRow::getCompletedAt).reversed())
          .limit(pageable.getPageSize())
          .collect(Collectors.toList());
    }

    private static <T> boolean matches(Optional<T> filter, T value) {
      return filter.map(expected -> expected.equals(value)).orElse(true);
    }
  }
}
