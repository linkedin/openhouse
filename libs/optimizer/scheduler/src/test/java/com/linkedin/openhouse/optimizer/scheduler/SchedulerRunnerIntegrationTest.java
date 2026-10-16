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

import com.linkedin.openhouse.optimizer.config.OptimizerDatabaseConfiguration;
import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.SnapshotMetrics;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import com.linkedin.openhouse.optimizer.scheduler.config.SchedulerConfig;
import com.linkedin.openhouse.optimizer.testing.MySqlContainerInitializer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@ActiveProfiles("test")
@ContextConfiguration(initializers = MySqlContainerInitializer.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class SchedulerRunnerIntegrationTest {
  private static final String RESULTS_ENDPOINT = "http://optimizer/operations";

  @Configuration
  @EnableAutoConfiguration
  @EntityScan(basePackageClasses = TableOperationsRow.class)
  @EnableJpaRepositories(basePackageClasses = TableOperationsRepository.class)
  @Import({SchedulerConfig.class, OptimizerDatabaseConfiguration.class})
  static class TestApplication {}

  @Autowired private SchedulerRunner runner;
  @Autowired private TableOperationsRepository operationsRepo;
  @Autowired private TableStatsRepository statsRepo;
  @MockBean private JobsServiceClient jobsClient;

  @BeforeEach
  void noCallerTransaction() {
    // A test transaction would hide missing production transaction boundaries.
    assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
  }

  @AfterEach
  void cleanUp() {
    operationsRepo.deleteAllInBatch();
    statsRepo.deleteAllInBatch();
  }

  @ParameterizedTest
  @EnumSource(OperationTypeDto.class)
  void schedule_groupsByDatabaseAndAccount_andCommitsEachJob(OperationTypeDto type) {
    TableOperationsRow a = seed(type, "db1", "a", "alice", 600_000L);
    TableOperationsRow b = seed(type, "db1", "b", "alice", 500_000L);
    TableOperationsRow c = seed(type, "db1", "c", "alice", 400_000L);
    TableOperationsRow d = seed(type, "db1", "d", "headless", 10L);
    TableOperationsRow e = seed(type, "db2", "e", "alice", 10L);
    TableOperationsRow duplicate =
        operationsRepo.save(
            a.toBuilder()
                .id(UUID.randomUUID().toString())
                .createdAt(a.getCreatedAt().plusSeconds(1))
                .build());
    stubSuccessfulLaunch();

    runner.schedule(type);

    assertSubmitted(type, "alice", a, c);
    assertSubmitted(type, "alice", b);
    assertSubmitted(type, "headless", d);
    assertSubmitted(type, "alice", e);
    assertThat(operationsRepo.findById(duplicate.getId())).isEmpty();
    verifyNoMoreInteractions(jobsClient);

    clearInvocations(jobsClient);
    runner.schedule(type);
    verifyNoInteractions(jobsClient);
  }

  @ParameterizedTest
  @EnumSource(OperationTypeDto.class)
  void schedule_missingCreator_leavesOperationUnclaimed(OperationTypeDto type) {
    TableOperationsRow missing = seed(type, "db1", "missing", null, 10L);
    TableOperationsRow valid = seed(type, "db1", "valid", "alice", 10L);
    stubSuccessfulLaunch();

    runner.schedule(type);

    assertSubmitted(type, "alice", valid);
    TableOperationsRow stored = operationsRepo.findById(missing.getId()).orElseThrow();
    assertThat(stored.getStatus()).isEqualTo(OperationStatus.PENDING);
    assertThat(stored.getScheduledAt()).isNull();
    assertThat(stored.getJobId()).isNull();
    verifyNoMoreInteractions(jobsClient);
  }

  @ParameterizedTest
  @EnumSource(OperationTypeDto.class)
  void schedule_failedLaunch_releasesClaimForRetry(OperationTypeDto type) {
    TableOperationsRow row = seed(type, "db1", "a", "alice", 10L);
    doAnswer(
            invocation -> {
              assertClaimed(invocation.getArgument(4));
              return Optional.empty();
            })
        .when(jobsClient)
        .launch(anyString(), anyString(), anyString(), anyList(), anyList(), anyString());

    runner.schedule(type);

    TableOperationsRow stored = operationsRepo.findById(row.getId()).orElseThrow();
    assertThat(stored.getStatus()).isEqualTo(OperationStatus.PENDING);
    assertThat(stored.getScheduledAt()).isNotNull();
    assertThat(stored.getJobId()).isNull();
    verify(jobsClient)
        .launch(
            anyString(),
            eq(type.name()),
            eq("alice"),
            eq(List.of("db1.a")),
            eq(List.of(row.getId())),
            eq(RESULTS_ENDPOINT));
    clearInvocations(jobsClient);
    stubSuccessfulLaunch();

    runner.schedule(type);

    assertSubmitted(type, "alice", row);
    verifyNoMoreInteractions(jobsClient);
  }

  private void stubSuccessfulLaunch() {
    doAnswer(
            invocation -> {
              List<String> ids = invocation.getArgument(4);
              assertClaimed(ids);
              return Optional.of("job-" + ids.get(0));
            })
        .when(jobsClient)
        .launch(anyString(), anyString(), anyString(), anyList(), anyList(), anyString());
  }

  private void assertClaimed(List<String> ids) {
    assertThat(ids).isNotEmpty();
    assertThat(operationsRepo.findAllById(ids))
        .hasSize(ids.size())
        .allSatisfy(
            row -> {
              assertThat(row.getStatus()).isEqualTo(OperationStatus.SCHEDULING);
              assertThat(row.getScheduledAt()).isNotNull();
              assertThat(row.getJobId()).isNull();
            });
  }

  private void assertSubmitted(
      OperationTypeDto type, String account, TableOperationsRow... operations) {
    List<String> ids =
        Arrays.stream(operations).map(TableOperationsRow::getId).collect(Collectors.toList());
    List<String> names =
        Arrays.stream(operations)
            .map(row -> row.getDatabaseName() + "." + row.getTableName())
            .collect(Collectors.toList());
    verify(jobsClient)
        .launch(
            anyString(), eq(type.name()), eq(account), eq(names), eq(ids), eq(RESULTS_ENDPOINT));
    List<TableOperationsRow> stored = operationsRepo.findAllById(ids);
    assertThat(stored)
        .hasSize(ids.size())
        .allSatisfy(
            row -> {
              assertThat(row.getStatus()).isEqualTo(OperationStatus.SCHEDULED);
              assertThat(row.getJobId()).isEqualTo("job-" + ids.get(0));
              assertThat(row.getScheduledAt()).isNotNull();
            });
    assertThat(stored)
        .extracting(TableOperationsRow::getScheduledAt)
        .containsOnly(stored.get(0).getScheduledAt());
  }

  private TableOperationsRow seed(
      OperationTypeDto type, String database, String table, String creator, long files) {
    String tableUuid = UUID.randomUUID().toString();
    Instant now = Instant.now();
    statsRepo.save(
        TableStatsRow.builder()
            .tableUuid(tableUuid)
            .databaseName(database)
            .tableName(table)
            .snapshot(SnapshotMetrics.builder().numCurrentFiles(files).build())
            .tableProperties(creator == null ? Map.of() : Map.of("openhouse.tableCreator", creator))
            .updatedAt(now)
            .build());
    return operationsRepo.save(
        TableOperationsRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(tableUuid)
            .databaseName(database)
            .tableName(table)
            .operationType(type.toDb())
            .status(OperationStatus.PENDING)
            .createdAt(now)
            .build());
  }
}
