package com.linkedin.openhouse.optimizer.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.optimizer.analyzer.AnalyzeRequest;
import com.linkedin.openhouse.optimizer.analyzer.AnalyzerRunner;
import com.linkedin.openhouse.optimizer.analyzer.ExpiredTableDeletionAnalyzer;
import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.TableOperationsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.repository.TableOperationsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import com.linkedin.openhouse.optimizer.scheduler.config.SchedulerConfig;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/**
 * Times the bulk repository reads of the analyzer's full scan over {@value
 * ExpiredTableDeletionScaleFixture#TABLE_COUNT} synthetic tables stored in MySQL, and runs the
 * expired table deletion analyzer and scheduler over {@value #FULL_PASS_TABLE_COUNT} of them.
 * {@link ExpiredTableDeletionScaleTest} runs all {@value
 * ExpiredTableDeletionScaleFixture#TABLE_COUNT} tables through both passes in memory.
 *
 * <p>The full pass uses fewer tables because the analyzer saves each new operation, and the
 * scheduler claims and marks each submitted job, in transactions of their own. Their run time on
 * MySQL therefore grows with the number of scheduled tables times the database round trip, and at
 * {@value ExpiredTableDeletionScaleFixture#TABLE_COUNT} tables the two passes take several minutes
 * on a local MySQL container.
 *
 * <p>The time limits are several times the run time observed on a laptop with a local MySQL
 * container, so the test does not fail on a slow build machine.
 */
@Tag("scale")
@ActiveProfiles("test")
@TestPropertySource(locations = "classpath:optimizer-mysql-testcontainers.properties")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ExpiredTableDeletionScaleIntegrationTest {
  private static final OperationTypeDto DELETION = OperationTypeDto.EXPIRED_TABLE_DELETION;
  private static final int ROWS_PER_INSERT = 1_000;
  private static final int FULL_PASS_TABLE_COUNT = 10_000;
  private static final Duration BULK_READ_TIME_LIMIT = Duration.ofSeconds(30);
  private static final Duration ANALYZER_PASS_TIME_LIMIT = Duration.ofSeconds(120);
  private static final Duration SCHEDULER_PASS_TIME_LIMIT = Duration.ofSeconds(180);

  @Configuration
  @EnableAutoConfiguration
  @EntityScan(basePackageClasses = TableOperationsRow.class)
  @EnableJpaRepositories(basePackageClasses = TableOperationsRepository.class)
  @Import(SchedulerConfig.class)
  static class TestApplication {}

  @Autowired private SchedulerRunner scheduler;
  @Autowired private TableStatsRepository statsRepo;
  @Autowired private TableOperationsRepository operationsRepo;
  @Autowired private TableOperationsHistoryRepository historyRepo;
  @Autowired private JdbcTemplate jdbcTemplate;
  @Autowired private ObjectMapper objectMapper;
  @MockBean private JobsServiceClient jobsClient;

  private final List<List<String>> launchedJobTables =
      Collections.synchronizedList(new ArrayList<>());

  @BeforeEach
  void recordLaunchedJobs() {
    doAnswer(
            invocation -> {
              launchedJobTables.add(List.copyOf(invocation.<List<String>>getArgument(3)));
              return Optional.of("job-" + invocation.<List<String>>getArgument(4).get(0));
            })
        .when(jobsClient)
        .launch(anyString(), anyString(), anyString(), anyList(), anyList(), anyString());
  }

  @AfterEach
  void cleanUp() {
    historyRepo.deleteAllInBatch();
    operationsRepo.deleteAllInBatch();
    statsRepo.deleteAllInBatch();
  }

  @Test
  void analyzerBulkReads_returnEveryRow_withinBoundedTime() {
    ExpiredTableDeletionScaleFixture fixture = seed(ExpiredTableDeletionScaleFixture.TABLE_COUNT);
    OperationType deletion = DELETION.toDb();
    List<String> databaseNames = statsRepo.findDistinctDatabaseNames();

    Duration statsReadTime =
        timedRead(
            "table_stats per database",
            () ->
                databaseNames.stream()
                    .mapToLong(
                        databaseName ->
                            statsRepo
                                .find(
                                    Optional.of(databaseName),
                                    Optional.empty(),
                                    Optional.empty(),
                                    Pageable.unpaged())
                                .size())
                    .sum(),
            fixture.getTableStats().size());
    Duration operationsReadTime =
        timedRead(
            "table_operations per database",
            () ->
                databaseNames.stream()
                    .mapToLong(
                        databaseName ->
                            operationsRepo
                                .find(
                                    Optional.of(deletion),
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.of(databaseName),
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty(),
                                    Pageable.unpaged())
                                .size())
                    .sum(),
            fixture.getOperations().size());
    Duration latestHistoryReadTime =
        timedRead(
            "latest table_operations_history",
            () -> (long) historyRepo.findLatest(deletion, Pageable.unpaged()).size(),
            fixture.getHistory().size());

    assertThat(databaseNames).hasSize(ExpiredTableDeletionScaleFixture.DATABASE_COUNT);
    assertThat(List.of(statsReadTime, operationsReadTime, latestHistoryReadTime))
        .allSatisfy(readTime -> assertThat(readTime).isLessThan(BULK_READ_TIME_LIMIT));
  }

  @Test
  void analyzerAndScheduler_submitExactlyTheDueTables_withinBoundedTime() {
    ExpiredTableDeletionScaleFixture fixture = seed(FULL_PASS_TABLE_COUNT);
    AnalyzerRunner analyzer =
        new AnalyzerRunner(
            List.of(new ExpiredTableDeletionAnalyzer()), statsRepo, operationsRepo, historyRepo);

    Duration analysisTime =
        timed(
            () ->
                analyzer.analyze(
                    AnalyzeRequest.builder().operationTypes(Set.of(DELETION)).build()));
    System.out.printf(
        "Analyzer pass over %d MySQL tables: %d ms%n",
        fixture.getTableStats().size(), analysisTime.toMillis());

    Set<String> fixtureOperationIds =
        fixture.getOperations().stream()
            .map(TableOperationsRow::getId)
            .collect(Collectors.toUnmodifiableSet());
    List<TableOperationsRow> newOperations =
        operationsRepo.findAll().stream()
            .filter(operation -> !fixtureOperationIds.contains(operation.getId()))
            .collect(Collectors.toList());
    assertThat(newOperations)
        .allSatisfy(
            operation -> assertThat(operation.getStatus()).isEqualTo(OperationStatus.PENDING));
    assertThat(newOperations)
        .extracting(ExpiredTableDeletionScaleFixture::fullyQualifiedName)
        .containsExactlyInAnyOrderElementsOf(fixture.getTablesAnalyzerSchedules());
    assertThat(analysisTime).isLessThan(ANALYZER_PASS_TIME_LIMIT);

    Duration schedulingTime = timed(() -> scheduler.schedule(DELETION));
    System.out.printf(
        "Scheduler pass over %d MySQL pending deletions: %d ms%n",
        fixture.getTablesSchedulerSubmits().size(), schedulingTime.toMillis());

    assertThat(launchedJobTables).allSatisfy(tables -> assertThat(tables).hasSize(1));
    assertThat(launchedJobTables.stream().flatMap(List::stream).collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(fixture.getTablesSchedulerSubmits());
    assertThat(
            operationsRepo.find(
                Optional.of(DELETION.toDb()),
                Optional.of(OperationStatus.PENDING),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Pageable.unpaged()))
        .isEmpty();
    assertThat(schedulingTime).isLessThan(SCHEDULER_PASS_TIME_LIMIT);
  }

  private ExpiredTableDeletionScaleFixture seed(int tableCount) {
    ExpiredTableDeletionScaleFixture fixture =
        ExpiredTableDeletionScaleFixture.generate(Instant.now(), tableCount);
    Duration seedingTime =
        timed(
            () -> {
              insertTableStats(fixture.getTableStats());
              insertOperations(fixture.getOperations());
              insertHistory(fixture.getHistory());
            });
    System.out.printf(
        "Seeded %d tables, %d operations, and %d history rows in %d ms%n",
        fixture.getTableStats().size(),
        fixture.getOperations().size(),
        fixture.getHistory().size(),
        seedingTime.toMillis());
    return fixture;
  }

  private void insertTableStats(List<TableStatsRow> rows) {
    insertInChunks(
        "table_stats (table_uuid, database_name, table_name, snapshot, table_properties,"
            + " updated_at)",
        rows,
        row ->
            Stream.of(
                row.getTableUuid(),
                row.getDatabaseName(),
                row.getTableName(),
                json(row.getSnapshot()),
                json(row.getTableProperties()),
                Timestamp.from(row.getUpdatedAt())));
  }

  private void insertOperations(List<TableOperationsRow> rows) {
    insertInChunks(
        "table_operations (id, table_uuid, database_name, table_name, operation_type, status,"
            + " created_at, scheduled_at, job_id, attempts, next_eligible_at)",
        rows,
        row ->
            Stream.of(
                row.getId(),
                row.getTableUuid(),
                row.getDatabaseName(),
                row.getTableName(),
                row.getOperationType().name(),
                row.getStatus().name(),
                Timestamp.from(row.getCreatedAt()),
                Optional.ofNullable(row.getScheduledAt()).map(Timestamp::from).orElse(null),
                row.getJobId(),
                row.getAttempts(),
                Optional.ofNullable(row.getNextEligibleAt()).map(Timestamp::from).orElse(null)));
  }

  private void insertHistory(List<TableOperationsHistoryRow> rows) {
    insertInChunks(
        "table_operations_history (id, table_uuid, database_name, table_name, operation_type,"
            + " completed_at, status)",
        rows,
        row ->
            Stream.of(
                row.getId(),
                row.getTableUuid(),
                row.getDatabaseName(),
                row.getTableName(),
                row.getOperationType().name(),
                Timestamp.from(row.getCompletedAt()),
                row.getStatus().name()));
  }

  /**
   * Inserts rows with multi-row INSERT statements of {@value #ROWS_PER_INSERT} rows each, which
   * seeds the tables in seconds where saving each entity through JPA takes minutes.
   */
  private <T> void insertInChunks(
      String tableAndColumns, List<T> rows, Function<T, Stream<Object>> columnValues) {
    long columnCount = tableAndColumns.chars().filter(character -> character == ',').count() + 1;
    String rowPlaceholders =
        Stream.generate(() -> "?").limit(columnCount).collect(Collectors.joining(", ", "(", ")"));
    IntStream.iterate(0, start -> start < rows.size(), start -> start + ROWS_PER_INSERT)
        .mapToObj(start -> rows.subList(start, Math.min(start + ROWS_PER_INSERT, rows.size())))
        .forEach(
            chunk ->
                jdbcTemplate.update(
                    "INSERT INTO "
                        + tableAndColumns
                        + " VALUES "
                        + String.join(", ", Collections.nCopies(chunk.size(), rowPlaceholders)),
                    chunk.stream().flatMap(columnValues).toArray()));
  }

  private String json(Object value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("Cannot serialize " + value, exception);
    }
  }

  private static Duration timedRead(String description, Supplier<Long> read, long expectedRows) {
    long start = System.nanoTime();
    long rowCount = read.get();
    Duration readTime = Duration.ofNanos(System.nanoTime() - start);
    System.out.printf("Read %d rows of %s in %d ms%n", rowCount, description, readTime.toMillis());
    assertThat(rowCount).as(description).isEqualTo(expectedRows);
    return readTime;
  }

  private static Duration timed(Runnable pass) {
    long start = System.nanoTime();
    pass.run();
    return Duration.ofNanos(System.nanoTime() - start);
  }
}
