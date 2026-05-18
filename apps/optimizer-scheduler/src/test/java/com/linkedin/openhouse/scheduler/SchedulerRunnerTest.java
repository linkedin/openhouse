package com.linkedin.openhouse.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.optimizer.db.SnapshotMetrics;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import com.linkedin.openhouse.scheduler.client.JobsServiceClient;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchedulerRunnerTest {

  private static final OperationType OFD = OperationType.ORPHAN_FILES_DELETION;
  private static final com.linkedin.openhouse.optimizer.db.OperationType OFD_DB =
      com.linkedin.openhouse.optimizer.db.OperationType.ORPHAN_FILES_DELETION;
  private static final com.linkedin.openhouse.optimizer.db.OperationStatus PENDING_DB =
      com.linkedin.openhouse.optimizer.db.OperationStatus.PENDING;
  private static final String OFD_STR = OFD.name();
  private static final String RESULTS_ENDPOINT = "http://localhost:8080/v1/table-operations";

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

  private TableOperationsRow pendingRow(String uuid, String db, String table) {
    return TableOperationsRow.builder()
        .id(UUID.randomUUID().toString())
        .tableUuid(uuid)
        .databaseName(db)
        .tableName(table)
        .operationType(OFD_DB)
        .status(PENDING_DB)
        .createdAt(java.time.Instant.now())
        .build();
  }

  private TableStatsRow statsRow(String uuid, long numCurrentFiles) {
    return TableStatsRow.builder()
        .tableUuid(uuid)
        .snapshot(SnapshotMetrics.builder().numCurrentFiles(numCurrentFiles).build())
        .build();
  }

  /** Stubs the bin packer to return one bin containing every candidate. */
  private void stubOneBinForAllCandidates() {
    when(binPacker.pack(anyList()))
        .thenAnswer(
            inv ->
                List.of(
                    new Bin(
                        OFD,
                        inv.<List<SchedulingCandidate>>getArgument(0).stream()
                            .map(SchedulingCandidate::getOperation)
                            .collect(Collectors.toList()))));
  }

  @Test
  void schedule_noPendingOps_noJobSubmitted() {
    when(operationsRepo.find(OFD_DB, PENDING_DB, null, null, null)).thenReturn(List.of());

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

    verify(operationsRepo, never()).find(any(), any(), any(), any(), any());
    verify(jobsClient, never()).launch(anyString(), anyString(), anyList(), anyList(), anyString());
  }

  @Test
  void schedule_singleBin_claimsAndMarksScheduled() {
    String uuid = UUID.randomUUID().toString();
    TableOperationsRow row = pendingRow(uuid, "db1", "tbl1");

    when(operationsRepo.find(OFD_DB, PENDING_DB, null, null, null)).thenReturn(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(uuid, 100_000L)));
    stubOneBinForAllCandidates();
    when(operationsRepo.markSchedulingBatch(anyList(), any())).thenReturn(1);
    when(operationsRepo.findClaimedIds(anyList(), any())).thenReturn(List.of(row.getId()));
    when(operationsRepo.markScheduledBatch(anyList(), anyString())).thenReturn(1);
    when(jobsClient.launch(anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-123"));

    runner.schedule(OFD);

    ArgumentCaptor<List<String>> ids = ArgumentCaptor.forClass(List.class);
    verify(operationsRepo).markSchedulingBatch(ids.capture(), any());
    assertThat(ids.getValue()).containsExactly(row.getId());

    verify(operationsRepo).markScheduledBatch(eq(List.of(row.getId())), eq("job-123"));
    verify(operationsRepo, never()).markPendingBatch(anyList());

    ArgumentCaptor<List<String>> tableNames = ArgumentCaptor.forClass(List.class);
    verify(jobsClient)
        .launch(anyString(), eq(OFD_STR), tableNames.capture(), anyList(), anyString());
    assertThat(tableNames.getValue()).containsExactly("db1.tbl1");
  }

  @Test
  void schedule_jobLaunchFails_marksPendingForRetry() {
    String uuid = UUID.randomUUID().toString();
    TableOperationsRow row = pendingRow(uuid, "db1", "tbl1");

    when(operationsRepo.find(OFD_DB, PENDING_DB, null, null, null)).thenReturn(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(uuid, 100L)));
    stubOneBinForAllCandidates();
    when(operationsRepo.markSchedulingBatch(anyList(), any())).thenReturn(1);
    when(operationsRepo.findClaimedIds(anyList(), any())).thenReturn(List.of(row.getId()));
    when(jobsClient.launch(anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.empty());
    when(operationsRepo.markPendingBatch(anyList())).thenReturn(1);

    runner.schedule(OFD);

    verify(operationsRepo).markSchedulingBatch(eq(List.of(row.getId())), any());
    verify(operationsRepo).markPendingBatch(eq(List.of(row.getId())));
    verify(operationsRepo, never()).markScheduledBatch(anyList(), anyString());
  }

  @Test
  void schedule_rowsAlreadyClaimed_skipsSubmit() {
    String uuid = UUID.randomUUID().toString();
    TableOperationsRow row = pendingRow(uuid, "db1", "tbl1");

    when(operationsRepo.find(OFD_DB, PENDING_DB, null, null, null)).thenReturn(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(uuid, 100L)));
    stubOneBinForAllCandidates();
    when(operationsRepo.markSchedulingBatch(anyList(), any())).thenReturn(0);
    when(operationsRepo.findClaimedIds(anyList(), any())).thenReturn(List.of());

    runner.schedule(OFD);

    verify(jobsClient, never()).launch(anyString(), anyString(), anyList(), anyList(), anyString());
    verify(operationsRepo, never()).markScheduledBatch(anyList(), anyString());
    verify(operationsRepo, never()).markPendingBatch(anyList());
  }

  @Test
  void schedule_cancelsDuplicatePendingBeforeClaim() {
    String uuid = UUID.randomUUID().toString();
    TableOperationsRow row1 = pendingRow(uuid, "db1", "tbl1");
    TableOperationsRow row2 = pendingRow(uuid, "db1", "tbl1");

    when(operationsRepo.find(OFD_DB, PENDING_DB, null, null, null)).thenReturn(List.of(row1, row2));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(uuid, 100L)));
    stubOneBinForAllCandidates();
    when(operationsRepo.markSchedulingBatch(anyList(), any())).thenReturn(2);
    when(operationsRepo.findClaimedIds(anyList(), any()))
        .thenReturn(List.of(row1.getId(), row2.getId()));
    when(operationsRepo.markScheduledBatch(anyList(), anyString())).thenReturn(2);
    when(jobsClient.launch(anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-789"));

    runner.schedule(OFD);

    verify(operationsRepo).cancelDuplicatePendingBatch(eq(OFD_DB), anyList());
  }

  @Test
  void schedule_partialClaim_launchesAndMarksOnlyClaimedSubset() {
    String uuidA = UUID.randomUUID().toString();
    String uuidB = UUID.randomUUID().toString();
    TableOperationsRow rowA = pendingRow(uuidA, "db1", "tblA");
    TableOperationsRow rowB = pendingRow(uuidB, "db1", "tblB");

    when(operationsRepo.find(OFD_DB, PENDING_DB, null, null, null)).thenReturn(List.of(rowA, rowB));
    when(statsRepo.findAllById(any()))
        .thenReturn(List.of(statsRow(uuidA, 100L), statsRow(uuidB, 100L)));
    stubOneBinForAllCandidates();
    // We submitted ids [A, B] to mark; only A was actually claimed (B owned by another instance).
    when(operationsRepo.markSchedulingBatch(anyList(), any())).thenReturn(1);
    when(operationsRepo.findClaimedIds(anyList(), any())).thenReturn(List.of(rowA.getId()));
    when(operationsRepo.markScheduledBatch(anyList(), anyString())).thenReturn(1);
    when(jobsClient.launch(anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-partial"));

    runner.schedule(OFD);

    // Job is launched for the claimed subset only.
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

    // markScheduledBatch is called only with the claimed id, not the unclaimed one.
    verify(operationsRepo).markScheduledBatch(eq(List.of(rowA.getId())), eq("job-partial"));
    verify(operationsRepo, never()).markScheduledBatch(eq(List.of(rowB.getId())), anyString());
    verify(operationsRepo, never()).markPendingBatch(anyList());
  }

  @Test
  void schedule_opsWithoutStats_skipped() {
    String withStats = UUID.randomUUID().toString();
    String missing = UUID.randomUUID().toString();
    TableOperationsRow withStatsRow = pendingRow(withStats, "db1", "tblA");
    TableOperationsRow missingRow = pendingRow(missing, "db1", "tblB");

    when(operationsRepo.find(OFD_DB, PENDING_DB, null, null, null))
        .thenReturn(List.of(withStatsRow, missingRow));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(withStats, 50L)));
    stubOneBinForAllCandidates();
    when(operationsRepo.markSchedulingBatch(anyList(), any())).thenReturn(1);
    when(operationsRepo.findClaimedIds(anyList(), any())).thenReturn(List.of(withStatsRow.getId()));
    when(operationsRepo.markScheduledBatch(anyList(), anyString())).thenReturn(1);
    when(jobsClient.launch(anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-skip"));

    runner.schedule(OFD);

    // Only the op with a stats row makes it into the claim batch.
    ArgumentCaptor<List<String>> ids = ArgumentCaptor.forClass(List.class);
    verify(operationsRepo).markSchedulingBatch(ids.capture(), any());
    assertThat(ids.getValue()).containsExactly(withStatsRow.getId());
  }

  @Test
  void schedule_allOpsWithoutStats_noJobSubmitted() {
    TableOperationsRow row = pendingRow(UUID.randomUUID().toString(), "db1", "tbl1");

    when(operationsRepo.find(OFD_DB, PENDING_DB, null, null, null)).thenReturn(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of());

    runner.schedule(OFD);

    verify(binPacker, never()).pack(anyList());
    verify(jobsClient, never()).launch(anyString(), anyString(), anyList(), anyList(), anyString());
  }
}
