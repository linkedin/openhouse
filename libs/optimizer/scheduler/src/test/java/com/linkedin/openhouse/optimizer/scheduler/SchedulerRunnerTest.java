package com.linkedin.openhouse.optimizer.scheduler;

import static com.linkedin.openhouse.optimizer.model.OperationTypeDto.ORPHAN_FILES_DELETION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.optimizer.binpack.FirstFitDecreasingBinPacker;
import com.linkedin.openhouse.optimizer.binpack.TotalFilesBinItem;
import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.SnapshotMetrics;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import com.linkedin.openhouse.optimizer.scheduler.account.AccountResolver;
import com.linkedin.openhouse.optimizer.scheduler.account.DummyAccountResolver;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchedulerRunnerTest {

  private static final String RESULTS_ENDPOINT = "http://localhost:8080/v1/optimizer/operations";

  @Mock private TableOperationsRepository operationsRepo;
  @Mock private TableStatsRepository statsRepo;
  @Mock private JobsServiceClient jobsClient;

  private SchedulerRunner runner;

  @BeforeEach
  void setUp() {
    // A real packer — the runner exercises the full pipeline against actual bucketing and the
    // packer's projection logic, while the IO is mocked.
    runner = newRunner(new DummyAccountResolver());
  }

  private SchedulerRunner newRunner(AccountResolver resolver) {
    return new SchedulerRunner(operationsRepo, statsRepo, jobsClient, resolver, RESULTS_ENDPOINT)
        .registerOperation(
            ORPHAN_FILES_DELETION,
            FirstFitDecreasingBinPacker.<TotalFilesBinItem>builder()
                .binItemSupplier(TotalFilesBinItem::new)
                .maxWeightPerBin(1_000_000L)
                .maxItemsPerBin(50)
                .build());
  }

  // ---- Stubbing helpers ----

  private void stubFindPending(List<TableOperationsRow> rows) {
    when(operationsRepo.find(
            eq(Optional.of(ORPHAN_FILES_DELETION.toDb())),
            eq(Optional.of(OperationStatus.PENDING)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenReturn(rows);
  }

  private void stubFindClaimed(List<TableOperationsRow> rows) {
    when(operationsRepo.find(
            eq(Optional.empty()),
            eq(Optional.of(OperationStatus.SCHEDULING)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any(),
            any(),
            any()))
        .thenReturn(rows);
  }

  private TableOperationsRow pendingRow(String uuid, String db, String table) {
    return TableOperationsRow.builder()
        .id(UUID.randomUUID().toString())
        .tableUuid(uuid)
        .databaseName(db)
        .tableName(table)
        .operationType(ORPHAN_FILES_DELETION.toDb())
        .status(OperationStatus.PENDING)
        .createdAt(Instant.now())
        .build();
  }

  private TableOperationsRow schedulingRow(TableOperationsRow source) {
    return source.toBuilder().status(OperationStatus.SCHEDULING).build();
  }

  private TableStatsRow statsRow(String uuid, long numCurrentFiles) {
    return statsRow(uuid, numCurrentFiles, "alice");
  }

  private TableStatsRow statsRow(String uuid, long numCurrentFiles, String creator) {
    return TableStatsRow.builder()
        .tableUuid(uuid)
        .tableProperties(Map.of("openhouse.tableCreator", creator))
        .snapshot(SnapshotMetrics.builder().numCurrentFiles(numCurrentFiles).build())
        .build();
  }

  // ---- Tests ----

  @Test
  void schedule_unknownOperationType_doesNotLaunch() {
    SchedulerRunner empty =
        new SchedulerRunner(
            operationsRepo, statsRepo, jobsClient, new DummyAccountResolver(), RESULTS_ENDPOINT);
    stubFindPending(List.of(pendingRow(UUID.randomUUID().toString(), "db1", "tbl1")));

    empty.schedule(ORPHAN_FILES_DELETION);

    // verifyNoInteractions(jobsClient);
    verify(jobsClient, never())
        .launch(anyString(), anyString(), anyString(), anyList(), anyList(), anyString());
  }

  @Test
  void getRegisteredOperationTypes_returnsRegisteredSet() {
    assertThat(runner.getRegisteredOperationTypes()).containsExactly(ORPHAN_FILES_DELETION);
  }

  @Test
  void schedule_noPendingOps_noJobSubmitted() {
    stubFindPending(List.of());

    runner.schedule(ORPHAN_FILES_DELETION);

    verify(jobsClient, never())
        .launch(anyString(), anyString(), anyString(), anyList(), anyList(), anyString());
  }

  @Test
  void schedule_allOpsWithoutStats_noJobSubmitted() {
    TableOperationsRow row = pendingRow(UUID.randomUUID().toString(), "db1", "tbl1");
    stubFindPending(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of());

    runner.schedule(ORPHAN_FILES_DELETION);

    verify(jobsClient, never())
        .launch(anyString(), anyString(), anyString(), anyList(), anyList(), anyString());
  }

  @Test
  void schedule_singleBin_claimsAndMarksScheduled() {
    String uuid = UUID.randomUUID().toString();
    TableOperationsRow row = pendingRow(uuid, "db1", "tbl1");

    stubFindPending(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(uuid, 100_000L)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(1);
    stubFindClaimed(List.of(schedulingRow(row)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.SCHEDULED), any(), any()))
        .thenReturn(1);
    when(jobsClient.launch(
            anyString(), anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-123"));

    runner.schedule(ORPHAN_FILES_DELETION);

    verify(operationsRepo)
        .updateBatch(
            eq(List.of(row.getId())),
            eq(OperationStatus.SCHEDULING),
            eq(OperationStatus.SCHEDULED),
            eq(Optional.empty()),
            eq(Optional.of("job-123")));
    verify(operationsRepo, never())
        .updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.PENDING), any(), any());

    ArgumentCaptor<String> jobName = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<List<String>> tableNames = ArgumentCaptor.forClass(List.class);
    verify(jobsClient)
        .launch(
            jobName.capture(),
            eq(ORPHAN_FILES_DELETION.name() + "_BATCH"),
            eq("alice"),
            tableNames.capture(),
            eq(List.of(row.getId())),
            eq(RESULTS_ENDPOINT));
    assertThat(jobName.getValue()).startsWith("batched-orphan_files_deletion-");
    assertThat(tableNames.getValue()).containsExactly("db1.tbl1");
  }

  @Test
  void schedule_jobLaunchFails_marksPendingForRetry() {
    String uuid = UUID.randomUUID().toString();
    TableOperationsRow row = pendingRow(uuid, "db1", "tbl1");

    stubFindPending(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(uuid, 100L)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(1);
    stubFindClaimed(List.of(schedulingRow(row)));
    when(jobsClient.launch(
            anyString(), anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.empty());
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.PENDING), any(), any()))
        .thenReturn(1);

    runner.schedule(ORPHAN_FILES_DELETION);

    verify(operationsRepo)
        .updateBatch(
            eq(List.of(row.getId())),
            eq(OperationStatus.SCHEDULING),
            eq(OperationStatus.PENDING),
            eq(Optional.empty()),
            eq(Optional.empty()));
    verify(operationsRepo, never())
        .updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.SCHEDULED), any(), any());
  }

  @Test
  void schedule_rowsAlreadyClaimed_skipsSubmit() {
    String uuid = UUID.randomUUID().toString();
    TableOperationsRow row = pendingRow(uuid, "db1", "tbl1");

    stubFindPending(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(uuid, 100L)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(0);
    stubFindClaimed(List.of());

    runner.schedule(ORPHAN_FILES_DELETION);

    verify(jobsClient, never())
        .launch(anyString(), anyString(), anyString(), anyList(), anyList(), anyString());
    verify(operationsRepo, never())
        .updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.SCHEDULED), any(), any());
    verify(operationsRepo, never())
        .updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.PENDING), any(), any());
  }

  @Test
  void schedule_cancelsDuplicatePendingPerCycle() {
    String uuid = UUID.randomUUID().toString();
    TableOperationsRow row1 = pendingRow(uuid, "db1", "tbl1");
    TableOperationsRow row2 = pendingRow(uuid, "db1", "tbl1");

    stubFindPending(List.of(row1, row2));
    when(operationsRepo.cancel(anyList())).thenReturn(1);
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(uuid, 100L)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(1);
    TableOperationsRow survivor = row1.getCreatedAt().isBefore(row2.getCreatedAt()) ? row1 : row2;
    if (row1.getCreatedAt().equals(row2.getCreatedAt())) {
      survivor = row1.getId().compareTo(row2.getId()) <= 0 ? row1 : row2;
    }
    stubFindClaimed(List.of(schedulingRow(survivor)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.SCHEDULED), any(), any()))
        .thenReturn(1);
    when(jobsClient.launch(
            anyString(), anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-dedup"));

    runner.schedule(ORPHAN_FILES_DELETION);

    ArgumentCaptor<List<String>> cancelled = ArgumentCaptor.forClass(List.class);
    verify(operationsRepo).cancel(cancelled.capture());
    assertThat(cancelled.getValue()).hasSize(1);
  }

  @Test
  void schedule_partialClaim_launchesAndMarksOnlyClaimedSubset() {
    String uuidA = UUID.randomUUID().toString();
    String uuidB = UUID.randomUUID().toString();
    TableOperationsRow rowA = pendingRow(uuidA, "db1", "tblA");
    TableOperationsRow rowB = pendingRow(uuidB, "db1", "tblB");

    stubFindPending(List.of(rowA, rowB));
    when(statsRepo.findAllById(any()))
        .thenReturn(List.of(statsRow(uuidA, 100L), statsRow(uuidB, 100L)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(1);
    // Only A actually claimed.
    stubFindClaimed(List.of(schedulingRow(rowA)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.SCHEDULED), any(), any()))
        .thenReturn(1);
    when(jobsClient.launch(
            anyString(), anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-partial"));

    runner.schedule(ORPHAN_FILES_DELETION);

    ArgumentCaptor<List<String>> launchedTableNames = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<String>> launchedOpIds = ArgumentCaptor.forClass(List.class);
    verify(jobsClient)
        .launch(
            anyString(),
            eq(ORPHAN_FILES_DELETION.name() + "_BATCH"),
            eq("alice"),
            launchedTableNames.capture(),
            launchedOpIds.capture(),
            eq(RESULTS_ENDPOINT));
    assertThat(launchedTableNames.getValue()).containsExactly("db1.tblA");
    assertThat(launchedOpIds.getValue()).containsExactly(rowA.getId());

    verify(operationsRepo)
        .updateBatch(
            eq(List.of(rowA.getId())),
            eq(OperationStatus.SCHEDULING),
            eq(OperationStatus.SCHEDULED),
            eq(Optional.empty()),
            eq(Optional.of("job-partial")));
  }

  @Test
  void schedule_opsWithoutStats_skipped() {
    String withStats = UUID.randomUUID().toString();
    String missing = UUID.randomUUID().toString();
    TableOperationsRow withStatsRow = pendingRow(withStats, "db1", "tblA");
    TableOperationsRow missingRow = pendingRow(missing, "db1", "tblB");

    stubFindPending(List.of(withStatsRow, missingRow));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(withStats, 50L)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(1);
    stubFindClaimed(List.of(schedulingRow(withStatsRow)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.SCHEDULED), any(), any()))
        .thenReturn(1);
    when(jobsClient.launch(
            anyString(), anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-skip"));

    runner.schedule(ORPHAN_FILES_DELETION);

    ArgumentCaptor<List<String>> ids = ArgumentCaptor.forClass(List.class);
    verify(operationsRepo)
        .updateBatch(
            ids.capture(),
            eq(OperationStatus.PENDING),
            eq(OperationStatus.SCHEDULING),
            any(),
            any());
    assertThat(ids.getValue()).containsExactly(withStatsRow.getId());
  }

  @Test
  void schedule_packsWithinDatabaseAndResolvedAccount() {
    runner =
        newRunner(Map.of("creator-a", "alice", "creator-b", "alice", "headless", "headless")::get);
    TableOperationsRow a = pendingRow("a", "db1", "a");
    TableOperationsRow b = pendingRow("b", "db1", "b");
    TableOperationsRow c = pendingRow("c", "db1", "c");
    TableOperationsRow d = pendingRow("d", "db1", "d");
    TableOperationsRow e = pendingRow("e", "db2", "e");
    List<TableOperationsRow> rows = List.of(a, b, c, d, e);
    stubFindPending(rows);
    when(statsRepo.findAllById(any()))
        .thenReturn(
            List.of(
                statsRow("a", 600_000L, "creator-a"),
                statsRow("b", 500_000L, "creator-a"),
                statsRow("c", 400_000L, "creator-b"),
                statsRow("d", 10L, "headless"),
                statsRow("e", 10L, "creator-a")));
    stubFindClaimed(rows.stream().map(this::schedulingRow).collect(Collectors.toList()));
    when(jobsClient.launch(
            anyString(), anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job"));

    runner.schedule(ORPHAN_FILES_DELETION);

    verify(jobsClient)
        .launch(
            anyString(),
            eq(ORPHAN_FILES_DELETION.name() + "_BATCH"),
            eq("alice"),
            eq(List.of("db1.a", "db1.c")),
            eq(List.of(a.getId(), c.getId())),
            eq(RESULTS_ENDPOINT));
    verify(jobsClient)
        .launch(
            anyString(),
            eq(ORPHAN_FILES_DELETION.name() + "_BATCH"),
            eq("alice"),
            eq(List.of("db1.b")),
            eq(List.of(b.getId())),
            eq(RESULTS_ENDPOINT));
    verify(jobsClient)
        .launch(
            anyString(),
            eq(ORPHAN_FILES_DELETION.name() + "_BATCH"),
            eq("headless"),
            eq(List.of("db1.d")),
            eq(List.of(d.getId())),
            eq(RESULTS_ENDPOINT));
    verify(jobsClient)
        .launch(
            anyString(),
            eq(ORPHAN_FILES_DELETION.name() + "_BATCH"),
            eq("alice"),
            eq(List.of("db2.e")),
            eq(List.of(e.getId())),
            eq(RESULTS_ENDPOINT));
    verifyNoMoreInteractions(jobsClient);
  }

  @ParameterizedTest
  @MethodSource("missingCreatorProperties")
  void schedule_missingCreator_doesNotClaim(Map<String, String> properties) {
    stubFindPending(List.of(pendingRow("a", "db1", "a")));
    when(statsRepo.findAllById(any()))
        .thenReturn(List.of(statsRow("a", 10L).toBuilder().tableProperties(properties).build()));

    runner.schedule(ORPHAN_FILES_DELETION);

    verifyNoInteractions(jobsClient);
    verify(operationsRepo, never()).updateBatch(anyList(), any(), any(), any(), any());
  }

  private static Stream<Map<String, String>> missingCreatorProperties() {
    return Stream.of(
        null,
        Map.of(),
        Collections.singletonMap("openhouse.tableCreator", null),
        Map.of("openhouse.tableCreator", ""),
        Map.of("openhouse.tableCreator", " "));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = " ")
  void schedule_emptyResolvedAccount_doesNotClaim(String account) {
    runner = newRunner(creator -> account);
    stubFindPending(List.of(pendingRow("a", "db1", "a")));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow("a", 10L)));

    runner.schedule(ORPHAN_FILES_DELETION);

    verifyNoInteractions(jobsClient);
    verify(operationsRepo, never()).updateBatch(anyList(), any(), any(), any(), any());
  }
}
