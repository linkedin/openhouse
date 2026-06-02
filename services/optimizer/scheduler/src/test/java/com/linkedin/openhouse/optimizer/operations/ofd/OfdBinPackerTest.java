package com.linkedin.openhouse.optimizer.operations.ofd;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
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
import com.linkedin.openhouse.optimizer.scheduler.binpack.Bin;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OfdBinPackerTest {

  private static final com.linkedin.openhouse.optimizer.db.OperationType OFD_DB =
      com.linkedin.openhouse.optimizer.db.OperationType.ORPHAN_FILES_DELETION;
  private static final String RESULTS_ENDPOINT = "http://localhost:8080/v1/optimizer/operations";
  private static final long MAX_FILES_PER_BIN = 1_000_000L;
  private static final int MAX_TABLES_PER_BIN = 50;

  @Mock private TableOperationsRepository operationsRepo;
  @Mock private TableStatsRepository statsRepo;
  @Mock private JobsServiceClient jobsClient;

  private OfdBinPacker packer;

  @BeforeEach
  void setUp() {
    packer =
        new OfdBinPacker(
            MAX_FILES_PER_BIN,
            MAX_TABLES_PER_BIN,
            operationsRepo,
            statsRepo,
            jobsClient,
            RESULTS_ENDPOINT);
  }

  // ---- Helpers ----

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

  private TableStatsRow statsRow(String uuid, long numCurrentFiles) {
    return TableStatsRow.builder()
        .tableUuid(uuid)
        .snapshot(SnapshotMetrics.builder().numCurrentFiles(numCurrentFiles).build())
        .build();
  }

  // ---- Tests ----

  @Test
  void prepare_noPending_returnsEmpty() {
    stubFindPending(List.of());

    List<Bin> bins = packer.prepare(Optional.empty(), Optional.empty());

    assertThat(bins).isEmpty();
    verify(statsRepo, never()).findAllById(any());
  }

  @Test
  void prepare_allOpsWithoutStats_returnsEmpty() {
    TableOperationsRow row = pendingRow(UUID.randomUUID().toString(), "db1", "tbl1");
    stubFindPending(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of());

    List<Bin> bins = packer.prepare(Optional.empty(), Optional.empty());

    assertThat(bins).isEmpty();
  }

  @Test
  void prepare_singleOpWithStats_returnsOneBin() {
    String uuid = UUID.randomUUID().toString();
    TableOperationsRow row = pendingRow(uuid, "db1", "tbl1");
    stubFindPending(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(uuid, 100L)));

    List<Bin> bins = packer.prepare(Optional.empty(), Optional.empty());

    assertThat(bins).hasSize(1);
  }

  @Test
  void prepare_cancelsDuplicatePendingPerCycle() {
    String uuid = UUID.randomUUID().toString();
    TableOperationsRow row1 = pendingRow(uuid, "db1", "tbl1");
    TableOperationsRow row2 = pendingRow(uuid, "db1", "tbl1");
    stubFindPending(List.of(row1, row2));
    when(operationsRepo.cancel(anyList())).thenReturn(1);
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(uuid, 100L)));

    packer.prepare(Optional.empty(), Optional.empty());

    ArgumentCaptor<List<String>> cancelled = ArgumentCaptor.forClass(List.class);
    verify(operationsRepo).cancel(cancelled.capture());
    assertThat(cancelled.getValue()).hasSize(1);
  }

  @Test
  void prepare_skipsOpsWithoutStats_includesOnlyThoseWithStats() {
    String withStats = UUID.randomUUID().toString();
    String missing = UUID.randomUUID().toString();
    TableOperationsRow withStatsRow = pendingRow(withStats, "db1", "tblA");
    TableOperationsRow missingRow = pendingRow(missing, "db1", "tblB");
    stubFindPending(List.of(withStatsRow, missingRow));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(withStats, 50L)));

    List<Bin> bins = packer.prepare(Optional.empty(), Optional.empty());

    assertThat(bins).hasSize(1);
  }

  @Test
  void prepare_packerReturnsBinsThatAreOfdBins() {
    String uuid = UUID.randomUUID().toString();
    TableOperationsRow row = pendingRow(uuid, "db1", "tbl1");
    stubFindPending(List.of(row));
    when(statsRepo.findAllById(any())).thenReturn(List.of(statsRow(uuid, 100L)));

    List<Bin> bins = packer.prepare(Optional.empty(), Optional.empty());

    assertThat(bins).allMatch(b -> b instanceof OfdBin);
  }

  @Test
  void getOperationType_returnsOrphanFilesDeletion() {
    assertThat(packer.getOperationType()).isEqualTo(OperationTypeDto.ORPHAN_FILES_DELETION);
  }
}
