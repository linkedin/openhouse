package com.linkedin.openhouse.optimizer.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.SnapshotMetrics;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import com.linkedin.openhouse.optimizer.scheduler.binpack.BinItem;
import com.linkedin.openhouse.optimizer.scheduler.binpack.BinPacker;
import com.linkedin.openhouse.optimizer.scheduler.binpack.FirstFitDecreasingBinPacker;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchedulerRunnerTest {

  private static final OperationTypeDto OFD = OperationTypeDto.ORPHAN_FILES_DELETION;
  private static final com.linkedin.openhouse.optimizer.db.OperationType OFD_DB =
      com.linkedin.openhouse.optimizer.db.OperationType.ORPHAN_FILES_DELETION;
  private static final String OFD_STR = OFD.name();
  private static final String RESULTS_ENDPOINT = "http://localhost:8080/v1/optimizer/operations";

  @Mock private TableOperationsRepository operationsRepo;
  @Mock private TableStatsRepository statsRepo;
  @Mock private JobsServiceClient jobsClient;
  @Mock private BinPacker binPacker;

  private SchedulerRunner runner;

  @BeforeEach
  void setUp() {
    runner =
        new SchedulerRunner(
            operationsRepo, statsRepo, jobsClient, Map.of(OFD, binPacker), RESULTS_ENDPOINT);
  }

  // ---- Stubbing helpers ----

  /** Stubs the initial "find PENDING" call. */
  private void stubFindPending(List<TableOperationsRow> rows) {
    when(operationsRepo.find(
            eq(Optional.of(OFD_DB)),
            eq(Optional.of(OperationStatus.PENDING)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenReturn(rows);
  }

  /** Stubs the post-claim "find SCHEDULING" call. */
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

  /**
   * Stubs the bin packer to put every input item into a single bin, by routing through a real FFD
   * packer with unbounded caps. Lets the test exercise the runner's projection (op → BinItem)
   * without bypassing Bin's package-private mutators.
   */
  private void stubOneBinForAllItems() {
    FirstFitDecreasingBinPacker realPacker =
        FirstFitDecreasingBinPacker.builder()
            .maxWeightPerBin(0L)
            .maxSizeBytesPerBin(0L)
            .maxItemsPerBin(0)
            .build();
    when(binPacker.pack(anyList()))
        .thenAnswer(inv -> realPacker.pack(inv.<List<BinItem>>getArgument(0)));
  }

  private TableOperationsRow pendingRow(String uuid, String db, String table) {
    return TableOperationsRow.builder()
        .id(UUID.randomUUID().toString())
        .tableUuid(uuid)
        .databaseName(db)
        .tableName(table)
        .operationType(OFD_DB)
        .status(OperationStatus.PENDING)
        .createdAt(Instant.now())
        .build();
  }

  private TableOperationsRow schedulingRow(TableOperationsRow source) {
    return source.toBuilder().status(OperationStatus.SCHEDULING).build();
  }

  private TableStatsRow statsRow(String uuid, long numCurrentFiles) {
    return TableStatsRow.builder()
        .tableUuid(uuid)
        .snapshot(SnapshotMetrics.builder().numCurrentFiles(numCurrentFiles).build())
        .build();
  }

  // ---- Tests ----

  @Test
  void schedule_noPendingOps_noJobSubmitted() {
    stubFindPending(List.of());

    runner.schedule(OFD);

    verify(jobsClient, never()).launch(anyString(), anyString(), anyList(), anyList(), anyString());
    verify(binPacker, never()).pack(anyList());
  }

  @Test
  void schedule_unknownOperationType_throws() {
    SchedulerRunner emptyRunner =
        new SchedulerRunner(operationsRepo, statsRepo, jobsClient, Map.of(), RESULTS_ENDPOINT);

    assertThatThrownBy(() -> emptyRunner.schedule(OFD))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No BinPacker registered");

    verify(operationsRepo, never()).find(any(), any(), any(), any(), any(), any(), any(), any());
    verify(jobsClient, never()).launch(anyString(), anyString(), anyList(), anyList(), anyString());
  }

  @Test
  void schedule_singleBin_claimsAndMarksScheduled() {
    String uuid = UUID.randomUUID().toString();
    TableOperationsRow row = pendingRow(uuid, "db1", "tbl1");

    stubFindPending(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(uuid, 100_000L)));
    stubOneBinForAllItems();
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(1);
    stubFindClaimed(List.of(schedulingRow(row)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.SCHEDULED), any(), any()))
        .thenReturn(1);
    when(jobsClient.launch(anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-123"));

    runner.schedule(OFD);

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

    ArgumentCaptor<List<String>> tableNames = ArgumentCaptor.forClass(List.class);
    verify(jobsClient)
        .launch(anyString(), eq(OFD_STR), tableNames.capture(), anyList(), anyString());
    assertThat(tableNames.getValue()).containsExactly("db1.tbl1");
  }

  @Test
  void schedule_jobLaunchFails_marksPendingForRetry() {
    String uuid = UUID.randomUUID().toString();
    TableOperationsRow row = pendingRow(uuid, "db1", "tbl1");

    stubFindPending(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(uuid, 100L)));
    stubOneBinForAllItems();
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(1);
    stubFindClaimed(List.of(schedulingRow(row)));
    when(jobsClient.launch(anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.empty());
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.PENDING), any(), any()))
        .thenReturn(1);

    runner.schedule(OFD);

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
    stubOneBinForAllItems();
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(0);
    stubFindClaimed(List.of());

    runner.schedule(OFD);

    verify(jobsClient, never()).launch(anyString(), anyString(), anyList(), anyList(), anyString());
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
    stubOneBinForAllItems();
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(1);
    // After dedup, only row1 (oldest by createdAt then id) survives.
    TableOperationsRow survivor = row1.getCreatedAt().isBefore(row2.getCreatedAt()) ? row1 : row2;
    if (row1.getCreatedAt().equals(row2.getCreatedAt())) {
      survivor = row1.getId().compareTo(row2.getId()) <= 0 ? row1 : row2;
    }
    stubFindClaimed(List.of(schedulingRow(survivor)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.SCHEDULED), any(), any()))
        .thenReturn(1);
    when(jobsClient.launch(anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-dedup"));

    runner.schedule(OFD);

    // Exactly one ID was cancelled (the duplicate).
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
    stubOneBinForAllItems();
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(1);
    // Only A actually claimed (B owned by another instance).
    stubFindClaimed(List.of(schedulingRow(rowA)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.SCHEDULED), any(), any()))
        .thenReturn(1);
    when(jobsClient.launch(anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-partial"));

    runner.schedule(OFD);

    ArgumentCaptor<List<String>> launchedTableNames = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<String>> launchedOpIds = ArgumentCaptor.forClass(List.class);
    verify(jobsClient)
        .launch(
            anyString(),
            anyString(),
            launchedTableNames.capture(),
            launchedOpIds.capture(),
            anyString());
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
    stubOneBinForAllItems();
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(1);
    stubFindClaimed(List.of(schedulingRow(withStatsRow)));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.SCHEDULED), any(), any()))
        .thenReturn(1);
    when(jobsClient.launch(anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-skip"));

    runner.schedule(OFD);

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
  void schedule_allOpsWithoutStats_noJobSubmitted() {
    TableOperationsRow row = pendingRow(UUID.randomUUID().toString(), "db1", "tbl1");

    stubFindPending(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of());

    runner.schedule(OFD);

    verify(binPacker, never()).pack(anyList());
    verify(jobsClient, never()).launch(anyString(), anyString(), anyList(), anyList(), anyString());
  }
}
