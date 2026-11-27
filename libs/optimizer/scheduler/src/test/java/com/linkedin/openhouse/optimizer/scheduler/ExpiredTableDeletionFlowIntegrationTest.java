package com.linkedin.openhouse.optimizer.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.linkedin.openhouse.optimizer.analyzer.AnalyzeRequest;
import com.linkedin.openhouse.optimizer.analyzer.AnalyzerRunner;
import com.linkedin.openhouse.optimizer.analyzer.ExpiredTableDeletionAnalyzer;
import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.SnapshotMetrics;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.HistoryStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.model.TemporaryTable;
import com.linkedin.openhouse.optimizer.repository.TableOperationsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import com.linkedin.openhouse.optimizer.scheduler.config.SchedulerConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/**
 * Runs the expired table deletion path from table stats to job submission: the analyzer creates
 * operations for expired temporary tables, and the scheduler launches one deletion job for each.
 * The job's result report is applied to the database the way the Optimizer service applies it.
 */
@ActiveProfiles("test")
@TestPropertySource(locations = "classpath:optimizer-mysql-testcontainers.properties")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ExpiredTableDeletionFlowIntegrationTest {
  private static final OperationTypeDto DELETION = OperationTypeDto.EXPIRED_TABLE_DELETION;
  private static final String RESULTS_ENDPOINT = "http://optimizer/operations";
  private static final Map<String, String> EXPIRED_TABLE_CREATORS =
      Map.of("expired_hour", "alice", "expired_day", "bob");

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
  @MockBean private JobsServiceClient jobsClient;

  private AnalyzerRunner analyzer;

  @BeforeEach
  void createAnalyzer() {
    analyzer =
        new AnalyzerRunner(
            List.of(new ExpiredTableDeletionAnalyzer(new SimpleMeterRegistry())),
            statsRepo,
            operationsRepo,
            historyRepo);
    doAnswer(invocation -> Optional.of("job-" + invocation.<List<String>>getArgument(4).get(0)))
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
  void expiredTemporaryTables_areEachSubmittedForDeletionOnce() {
    Instant now = Instant.now();
    seedTable("expired_hour", "alice", temporary(now.minus(Duration.ofHours(1))));
    seedTable("expired_day", "bob", temporary(now.minus(Duration.ofDays(1))));
    seedTable("not_expired", "alice", temporary(now.plus(Duration.ofHours(1))));
    seedTable("ordinary", "alice", Map.of());

    analyzeAndSchedule();

    List<TableOperationsRow> operations = operationsRepo.findAll();
    assertThat(operations)
        .extracting(TableOperationsRow::getTableName)
        .containsExactlyInAnyOrderElementsOf(EXPIRED_TABLE_CREATORS.keySet());
    assertThat(operations)
        .allSatisfy(row -> assertThat(row.getStatus()).isEqualTo(OperationStatus.SCHEDULED));
    operations.forEach(this::assertLaunchedAlone);
    verifyNoMoreInteractions(jobsClient);

    clearInvocations(jobsClient);
    analyzeAndSchedule();

    assertThat(operationsRepo.findAll()).hasSameSizeAs(operations);
    verifyNoInteractions(jobsClient);

    // Each job reports SUCCESS after the tables expired. Once the retry interval it sets has
    // passed, the SUCCESS alone keeps the deleted tables from being scheduled again.
    operations.forEach(this::reportSuccessAndPassRetryInterval);
    analyzeAndSchedule();

    assertThat(operationsRepo.findAll()).hasSameSizeAs(operations);
    verifyNoInteractions(jobsClient);
  }

  private void analyzeAndSchedule() {
    analyzer.analyze(AnalyzeRequest.builder().operationTypes(Set.of(DELETION)).build());
    scheduler.schedule(DELETION);
  }

  private void assertLaunchedAlone(TableOperationsRow operation) {
    verify(jobsClient)
        .launch(
            anyString(),
            eq("EXPIRED_TABLE_DELETION_BATCH"),
            eq(EXPIRED_TABLE_CREATORS.get(operation.getTableName())),
            eq(List.of("db1." + operation.getTableName())),
            eq(List.of(operation.getId())),
            eq(RESULTS_ENDPOINT));
  }

  private void reportSuccessAndPassRetryInterval(TableOperationsRow operation) {
    historyRepo.save(
        TableOperationsHistoryDto.builder()
            .id(operation.getId())
            .tableUuid(operation.getTableUuid())
            .databaseName(operation.getDatabaseName())
            .tableName(operation.getTableName())
            .operationType(DELETION)
            .completedAt(Instant.now())
            .status(HistoryStatusDto.SUCCESS)
            .build()
            .toRow());
    operationsRepo.save(
        operation.toBuilder().nextEligibleAt(Instant.now().minus(Duration.ofDays(1))).build());
  }

  private static Map<String, String> temporary(Instant expirationTime) {
    return Map.of(
        TemporaryTable.TEMPORARY_PROPERTY,
        "true",
        TemporaryTable.EXPIRATION_TIME_PROPERTY,
        DateTimeFormatter.ISO_INSTANT.format(expirationTime.truncatedTo(ChronoUnit.SECONDS)));
  }

  private void seedTable(String table, String creator, Map<String, String> properties) {
    Map<String, String> tableProperties = new HashMap<>(properties);
    tableProperties.put("openhouse.tableCreator", creator);
    statsRepo.save(
        TableStatsRow.builder()
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName(table)
            .snapshot(SnapshotMetrics.builder().numCurrentFiles(10L).build())
            .tableProperties(tableProperties)
            .updatedAt(Instant.now())
            .build());
  }
}
