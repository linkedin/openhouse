package com.linkedin.openhouse.optimizer.operations.ofd;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.scheduler.client.JobsServiceClient;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OfdBinTest {

  private static final String RESULTS_ENDPOINT = "http://localhost:8080/v1/optimizer/operations";

  @Mock private TableOperationsRepository operationsRepo;
  @Mock private JobsServiceClient jobsClient;

  private static OfdBinItem item(String fqtn) {
    return new OfdBinItem(fqtn, UUID.randomUUID().toString(), 100L);
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

  private TableOperationsRow schedulingRow(String opId) {
    return TableOperationsRow.builder()
        .id(opId)
        .tableUuid(UUID.randomUUID().toString())
        .databaseName("db")
        .tableName("tbl")
        .operationType(OperationType.ORPHAN_FILES_DELETION)
        .status(OperationStatus.SCHEDULING)
        .createdAt(Instant.now())
        .build();
  }

  @Test
  void schedule_singleBin_claimsAndMarksScheduled() {
    OfdBinItem one = item("db1.tbl1");
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(1);
    stubFindClaimed(List.of(schedulingRow(one.getOperationId())));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.SCHEDULED), any(), any()))
        .thenReturn(1);
    when(jobsClient.launch(anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-123"));

    new OfdBin(List.of(one), operationsRepo, jobsClient, RESULTS_ENDPOINT).schedule();

    verify(operationsRepo)
        .updateBatch(
            eq(List.of(one.getOperationId())),
            eq(OperationStatus.SCHEDULING),
            eq(OperationStatus.SCHEDULED),
            eq(Optional.empty()),
            eq(Optional.of("job-123")));
    verify(operationsRepo, never())
        .updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.PENDING), any(), any());

    ArgumentCaptor<List<String>> tableNames = ArgumentCaptor.forClass(List.class);
    verify(jobsClient)
        .launch(
            anyString(), eq("ORPHAN_FILES_DELETION"), tableNames.capture(), anyList(), anyString());
    assertThat(tableNames.getValue()).containsExactly("db1.tbl1");
  }

  @Test
  void schedule_jobLaunchFails_revertsToPending() {
    OfdBinItem one = item("db1.tbl1");
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(1);
    stubFindClaimed(List.of(schedulingRow(one.getOperationId())));
    when(jobsClient.launch(anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.empty());
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.PENDING), any(), any()))
        .thenReturn(1);

    new OfdBin(List.of(one), operationsRepo, jobsClient, RESULTS_ENDPOINT).schedule();

    verify(operationsRepo)
        .updateBatch(
            eq(List.of(one.getOperationId())),
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
    OfdBinItem one = item("db1.tbl1");
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(0);
    stubFindClaimed(List.of());

    new OfdBin(List.of(one), operationsRepo, jobsClient, RESULTS_ENDPOINT).schedule();

    verify(jobsClient, never()).launch(anyString(), anyString(), anyList(), anyList(), anyString());
    verify(operationsRepo, never())
        .updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.SCHEDULED), any(), any());
    verify(operationsRepo, never())
        .updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.PENDING), any(), any());
  }

  @Test
  void schedule_partialClaim_launchesOnlyClaimedSubset() {
    OfdBinItem a = item("db1.tblA");
    OfdBinItem b = item("db1.tblB");
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.PENDING), eq(OperationStatus.SCHEDULING), any(), any()))
        .thenReturn(1);
    // Only A actually claimed.
    stubFindClaimed(List.of(schedulingRow(a.getOperationId())));
    when(operationsRepo.updateBatch(
            anyList(), eq(OperationStatus.SCHEDULING), eq(OperationStatus.SCHEDULED), any(), any()))
        .thenReturn(1);
    when(jobsClient.launch(anyString(), anyString(), anyList(), anyList(), anyString()))
        .thenReturn(Optional.of("job-partial"));

    new OfdBin(List.of(a, b), operationsRepo, jobsClient, RESULTS_ENDPOINT).schedule();

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
    assertThat(launchedOpIds.getValue()).containsExactly(a.getOperationId());
  }
}
