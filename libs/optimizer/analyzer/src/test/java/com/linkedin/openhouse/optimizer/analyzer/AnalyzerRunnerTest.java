package com.linkedin.openhouse.optimizer.analyzer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.optimizer.db.TableOperationsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.repository.TableOperationsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AnalyzerRunnerTest {

  private static final OperationTypeDto OFD_TYPE = OperationTypeDto.ORPHAN_FILES_DELETION;
  private static final com.linkedin.openhouse.optimizer.db.OperationType OFD_DB =
      com.linkedin.openhouse.optimizer.db.OperationType.ORPHAN_FILES_DELETION;
  private static final String DB = "db1";

  @Mock private TableStatsRepository statsRepo;
  @Mock private TableOperationsRepository operationsRepo;
  @Mock private TableOperationsHistoryRepository historyRepo;
  @Mock private OperationAnalyzer analyzer;

  private AnalyzerRunner runner;

  @BeforeEach
  void setUp() {
    runner = new AnalyzerRunner(List.of(analyzer), statsRepo, operationsRepo, historyRepo);
    // Lenient: the triggersOnCommit pre-filter can short-circuit analyzeTable before the type is
    // read (see analyzeTable_skipsLoadAndSave_whenNoAnalyzerTriggersOnCommit).
    lenient().when(analyzer.getOperationType()).thenReturn(OFD_TYPE);
    // Only the full-scan entry point resolves databases; analyzeTable passes the db through.
    lenient().when(statsRepo.findDistinctDatabaseNames()).thenReturn(List.of(DB));
  }

  @Test
  void analyzeTable_usesInMemoryStats_insertsPendingOp_withoutReadingTableStats() {
    TableDto table =
        TableDto.builder().tableUuid("uuid-1").databaseName(DB).tableId("tbl1").build();

    when(operationsRepo.find(
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.of("uuid-1")),
            eq(Optional.of(DB)),
            eq(Optional.of("tbl1")),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenReturn(Collections.emptyList());
    when(historyRepo.find(eq("uuid-1"), any())).thenReturn(Collections.emptyList());
    when(analyzer.triggersOnCommit(table)).thenReturn(true);
    when(analyzer.isEnabled(table)).thenReturn(true);
    when(analyzer.shouldSchedule(table, Optional.empty(), Optional.empty(), 0)).thenReturn(true);

    runner.analyze(AnalyzeRequest.builder().table(table).build());

    ArgumentCaptor<TableOperationsRow> captor = ArgumentCaptor.forClass(TableOperationsRow.class);
    verify(operationsRepo).save(captor.capture());
    TableOperationsRow saved = captor.getValue();
    assertThat(saved.getTableUuid()).isEqualTo("uuid-1");
    assertThat(saved.getTableName()).isEqualTo("tbl1");
    assertThat(saved.getOperationType()).isEqualTo(OFD_DB);
    assertThat(saved.getStatus())
        .isEqualTo(com.linkedin.openhouse.optimizer.db.OperationStatus.PENDING);
    // The point of the in-memory path: the commit-driven trigger never re-reads table_stats.
    verify(statsRepo, never()).find(any(), any(), any(), any());
  }

  @Test
  void analyzeTable_skipsLoadAndSave_whenNoAnalyzerTriggersOnCommit() {
    TableDto table =
        TableDto.builder().tableUuid("uuid-1").databaseName(DB).tableId("tbl1").build();
    when(analyzer.triggersOnCommit(table)).thenReturn(false);

    runner.analyze(AnalyzeRequest.builder().table(table).build());

    // Pre-filter short-circuits before any DB load or save.
    verify(operationsRepo, never()).find(any(), any(), any(), any(), any(), any(), any(), any());
    verify(historyRepo, never()).find(any(), any());
    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_countsConsecutiveFailures_fromRecentSubmissions_andPassesToShouldSchedule() {
    TableStatsRow statsEntity =
        TableStatsRow.builder().tableUuid("uuid-1").databaseName(DB).tableName("tbl1").build();
    TableDto expectedTable = TableDto.fromRow(statsEntity);

    when(statsRepo.find(eq(Optional.of(DB)), eq(Optional.empty()), eq(Optional.empty()), any()))
        .thenReturn(List.of(statsEntity));
    when(operationsRepo.find(
            eq(Optional.of(OFD_DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.of(DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenReturn(Collections.emptyList());
    // Latest run FAILED → not healthy → the runner walks recent submissions to measure the streak.
    when(historyRepo.findLatest(eq(OFD_DB), any()))
        .thenReturn(List.of(failedHistoryRow("op-1", Instant.now().minusSeconds(100))));
    // Three submissions, each with a FAILED outcome (history shares the submission's op id).
    when(operationsRepo.findRecentSubmissions(eq("uuid-1"), eq(OFD_DB), any()))
        .thenReturn(
            List.of(
                submittedOpRow("op-1", Instant.now().minusSeconds(100)),
                submittedOpRow("op-2", Instant.now().minusSeconds(200)),
                submittedOpRow("op-3", Instant.now().minusSeconds(300))));
    when(historyRepo.findRecent(eq("uuid-1"), eq(OFD_DB), any()))
        .thenReturn(
            List.of(
                failedHistoryRow("op-1", Instant.now().minusSeconds(100)),
                failedHistoryRow("op-2", Instant.now().minusSeconds(200)),
                failedHistoryRow("op-3", Instant.now().minusSeconds(300))));
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);
    when(analyzer.shouldSchedule(eq(expectedTable), any(), any(), eq(3))).thenReturn(false);

    runner.analyze(AnalyzeRequest.builder().operationTypes(java.util.Set.of(OFD_TYPE)).build());

    // The runner derived consecutiveFailures=3 from the recent submissions and passed it through.
    verify(analyzer).shouldSchedule(eq(expectedTable), any(), any(), eq(3));
    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_countsLogicalTimeout_asNonSuccess() {
    TableStatsRow statsEntity =
        TableStatsRow.builder().tableUuid("uuid-1").databaseName(DB).tableName("tbl1").build();
    TableDto expectedTable = TableDto.fromRow(statsEntity);
    TableOperationsRow staleOp = submittedOpRow("op-dead", Instant.now().minusSeconds(20_000));

    when(statsRepo.find(eq(Optional.of(DB)), eq(Optional.empty()), eq(Optional.empty()), any()))
        .thenReturn(List.of(statsEntity));
    // Current op is a stale SCHEDULED submission with no history → a logical timeout, no history
    // row.
    when(operationsRepo.find(
            eq(Optional.of(OFD_DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.of(DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenReturn(List.of(staleOp));
    when(historyRepo.findLatest(eq(OFD_DB), any())).thenReturn(Collections.emptyList());
    when(operationsRepo.findRecentSubmissions(eq("uuid-1"), eq(OFD_DB), any()))
        .thenReturn(List.of(staleOp));
    when(historyRepo.findRecent(eq("uuid-1"), eq(OFD_DB), any()))
        .thenReturn(Collections.emptyList());
    when(analyzer.isActiveOpTimedOut(any())).thenReturn(true);
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);
    when(analyzer.shouldSchedule(eq(expectedTable), any(), any(), eq(1))).thenReturn(false);

    runner.analyze(AnalyzeRequest.builder().operationTypes(java.util.Set.of(OFD_TYPE)).build());

    // The timed-out submission with no history counts as one non-success.
    verify(analyzer).shouldSchedule(eq(expectedTable), any(), any(), eq(1));
  }

  private static TableOperationsRow submittedOpRow(String id, Instant scheduledAt) {
    return TableOperationsRow.builder()
        .id(id)
        .status(com.linkedin.openhouse.optimizer.db.OperationStatus.SCHEDULED)
        .tableUuid("uuid-1")
        .databaseName(DB)
        .tableName("tbl1")
        .operationType(OFD_DB)
        .createdAt(scheduledAt.minusSeconds(100))
        .scheduledAt(scheduledAt)
        .jobId("job-" + id)
        .build();
  }

  private static TableOperationsHistoryRow failedHistoryRow(String id, Instant completedAt) {
    return TableOperationsHistoryRow.builder()
        .id(id)
        .tableUuid("uuid-1")
        .databaseName(DB)
        .tableName("tbl1")
        .operationType(OFD_DB)
        .completedAt(completedAt)
        .status(com.linkedin.openhouse.optimizer.db.HistoryStatus.FAILED)
        .build();
  }

  @Test
  void analyze_insertsNewRow_forEligibleTableWithNoExistingOp() {
    TableStatsRow statsEntity =
        TableStatsRow.builder().tableUuid("uuid-1").databaseName(DB).tableName("tbl1").build();

    TableDto expectedTable = TableDto.fromRow(statsEntity);

    when(statsRepo.find(eq(Optional.of(DB)), eq(Optional.empty()), eq(Optional.empty()), any()))
        .thenReturn(List.of(statsEntity));
    when(operationsRepo.find(
            eq(Optional.of(OFD_DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.of(DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenReturn(Collections.emptyList());
    when(historyRepo.findLatest(eq(OFD_DB), any())).thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);
    when(analyzer.shouldSchedule(expectedTable, Optional.empty(), Optional.empty(), 0))
        .thenReturn(true);

    runner.analyze(AnalyzeRequest.builder().operationTypes(java.util.Set.of(OFD_TYPE)).build());

    ArgumentCaptor<TableOperationsRow> captor = ArgumentCaptor.forClass(TableOperationsRow.class);
    verify(operationsRepo).save(captor.capture());
    TableOperationsRow saved = captor.getValue();
    assertThat(saved.getTableUuid()).isEqualTo("uuid-1");
    assertThat(saved.getDatabaseName()).isEqualTo(DB);
    assertThat(saved.getTableName()).isEqualTo("tbl1");
    assertThat(saved.getOperationType()).isEqualTo(OFD_DB);
    assertThat(saved.getStatus())
        .isEqualTo(com.linkedin.openhouse.optimizer.db.OperationStatus.PENDING);
    assertThat(saved.getId()).isNotNull();
  }

  @Test
  void analyze_noOp_whenCadencePolicyReturnsFalseForPending() {
    TableStatsRow statsEntity =
        TableStatsRow.builder().tableUuid("uuid-1").databaseName(DB).tableName("tbl1").build();

    TableDto expectedTable = TableDto.fromRow(statsEntity);

    TableOperationsRow existingEntity =
        TableOperationsRow.builder()
            .id("existing-op-id")
            .status(com.linkedin.openhouse.optimizer.db.OperationStatus.PENDING)
            .tableUuid("uuid-1")
            .operationType(OFD_DB)
            .createdAt(Instant.now())
            .build();

    when(statsRepo.find(eq(Optional.of(DB)), eq(Optional.empty()), eq(Optional.empty()), any()))
        .thenReturn(List.of(statsEntity));
    when(operationsRepo.find(
            eq(Optional.of(OFD_DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.of(DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenReturn(List.of(existingEntity));
    when(historyRepo.findLatest(eq(OFD_DB), any())).thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);

    TableOperationDto existingOp = TableOperationDto.fromRow(existingEntity);
    when(analyzer.shouldSchedule(expectedTable, Optional.of(existingOp), Optional.empty(), 0))
        .thenReturn(false);

    runner.analyze(AnalyzeRequest.builder().operationTypes(java.util.Set.of(OFD_TYPE)).build());

    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_skipsTable_whenNotEnabled() {
    TableStatsRow statsEntity =
        TableStatsRow.builder().tableUuid("uuid-1").databaseName(DB).build();

    TableDto expectedTable = TableDto.fromRow(statsEntity);

    when(statsRepo.find(eq(Optional.of(DB)), eq(Optional.empty()), eq(Optional.empty()), any()))
        .thenReturn(List.of(statsEntity));
    when(operationsRepo.find(
            eq(Optional.of(OFD_DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.of(DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenReturn(Collections.emptyList());
    when(historyRepo.findLatest(eq(OFD_DB), any())).thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(expectedTable)).thenReturn(false);

    runner.analyze(AnalyzeRequest.builder().operationTypes(java.util.Set.of(OFD_TYPE)).build());

    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_skipsTable_whenShouldScheduleReturnsFalse() {
    TableStatsRow statsEntity =
        TableStatsRow.builder().tableUuid("uuid-1").databaseName(DB).build();

    TableDto expectedTable = TableDto.fromRow(statsEntity);

    TableOperationsRow scheduled =
        TableOperationsRow.builder()
            .id("op-id")
            .status(com.linkedin.openhouse.optimizer.db.OperationStatus.SCHEDULED)
            .tableUuid("uuid-1")
            .operationType(OFD_DB)
            .createdAt(Instant.now())
            .build();

    when(statsRepo.find(eq(Optional.of(DB)), eq(Optional.empty()), eq(Optional.empty()), any()))
        .thenReturn(List.of(statsEntity));
    when(operationsRepo.find(
            eq(Optional.of(OFD_DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.of(DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenReturn(List.of(scheduled));
    when(historyRepo.findLatest(eq(OFD_DB), any())).thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);

    TableOperationDto scheduledOp = TableOperationDto.fromRow(scheduled);
    when(analyzer.shouldSchedule(expectedTable, Optional.of(scheduledOp), Optional.empty(), 0))
        .thenReturn(false);

    runner.analyze(AnalyzeRequest.builder().operationTypes(java.util.Set.of(OFD_TYPE)).build());

    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_skipsTable_whenTableUuidIsNull() {
    TableStatsRow statsEntity = TableStatsRow.builder().databaseName(DB).build();

    when(statsRepo.find(eq(Optional.of(DB)), eq(Optional.empty()), eq(Optional.empty()), any()))
        .thenReturn(List.of(statsEntity));
    when(operationsRepo.find(
            eq(Optional.of(OFD_DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.of(DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenReturn(Collections.emptyList());
    when(historyRepo.findLatest(any(), any())).thenReturn(Collections.emptyList());

    runner.analyze(AnalyzeRequest.builder().operationTypes(java.util.Set.of(OFD_TYPE)).build());

    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyzeRequest_withNonMatchingOperationType_isNoOp() {
    // The only registered analyzer is OFD; filtering to STATS selects nothing.
    runner.analyze(
        AnalyzeRequest.builder()
            .operationTypes(java.util.Set.of(OperationTypeDto.TABLE_STATS_COLLECTION))
            .build());

    verify(statsRepo, never()).findDistinctDatabaseNames();
    verify(statsRepo, never()).find(any(), any(), any(), any());
    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyzeRequest_withTable_dispatchesToCommitPath_withoutReadingTableStats() {
    TableDto table =
        TableDto.builder().tableUuid("uuid-1").databaseName(DB).tableId("tbl1").build();
    when(operationsRepo.find(
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.of("uuid-1")),
            eq(Optional.of(DB)),
            eq(Optional.of("tbl1")),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenReturn(Collections.emptyList());
    when(historyRepo.find(eq("uuid-1"), any())).thenReturn(Collections.emptyList());
    when(analyzer.triggersOnCommit(table)).thenReturn(true);
    when(analyzer.isEnabled(table)).thenReturn(true);
    when(analyzer.shouldSchedule(table, Optional.empty(), Optional.empty(), 0)).thenReturn(true);

    runner.analyze(AnalyzeRequest.builder().table(table).build());

    verify(operationsRepo).save(any());
    // Commit path uses the in-memory stats; it never scans table_stats or resolves databases.
    verify(statsRepo, never()).find(any(), any(), any(), any());
    verify(statsRepo, never()).findDistinctDatabaseNames();
  }

  @Test
  void analyzeRequest_withDatabaseFilter_scansOnlyThatDatabase_withoutResolvingAllDatabases() {
    TableStatsRow statsEntity =
        TableStatsRow.builder().tableUuid("uuid-1").databaseName(DB).tableName("tbl1").build();
    TableDto expectedTable = TableDto.fromRow(statsEntity);

    when(statsRepo.find(eq(Optional.of(DB)), eq(Optional.empty()), eq(Optional.empty()), any()))
        .thenReturn(List.of(statsEntity));
    when(operationsRepo.find(
            eq(Optional.of(OFD_DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.of(DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenReturn(Collections.emptyList());
    when(historyRepo.findLatest(eq(OFD_DB), any())).thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);
    when(analyzer.shouldSchedule(expectedTable, Optional.empty(), Optional.empty(), 0))
        .thenReturn(true);

    runner.analyze(AnalyzeRequest.builder().databaseName(DB).build());

    verify(operationsRepo).save(any());
    // A supplied database filter short-circuits the all-databases resolution.
    verify(statsRepo, never()).findDistinctDatabaseNames();
  }
}
