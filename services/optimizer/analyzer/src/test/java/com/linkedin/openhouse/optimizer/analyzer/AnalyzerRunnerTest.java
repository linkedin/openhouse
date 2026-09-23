package com.linkedin.openhouse.optimizer.analyzer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.optimizer.db.AnalyzerRunStateRow;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.OperationTypeDto;
import com.linkedin.openhouse.optimizer.model.TableDto;
import com.linkedin.openhouse.optimizer.model.TableOperationDto;
import com.linkedin.openhouse.optimizer.repository.AnalyzerRunStateRepository;
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
  @Mock private AnalyzerRunStateRepository runStateRepo;
  @Mock private OperationAnalyzer analyzer;

  private AnalyzerRunner runner;

  @BeforeEach
  void setUp() {
    runner =
        new AnalyzerRunner(
            List.of(analyzer), statsRepo, operationsRepo, historyRepo, runStateRepo, 1);
    when(analyzer.getOperationType()).thenReturn(OFD_TYPE);
    lenient().when(statsRepo.findDistinctDatabaseNames()).thenReturn(List.of(DB));
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
    when(analyzer.shouldSchedule(expectedTable, Optional.empty(), Optional.empty()))
        .thenReturn(true);

    runner.analyze(OFD_TYPE);

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
    when(analyzer.shouldSchedule(expectedTable, Optional.of(existingOp), Optional.empty()))
        .thenReturn(false);

    runner.analyze(OFD_TYPE);

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

    runner.analyze(OFD_TYPE);

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
    when(analyzer.shouldSchedule(expectedTable, Optional.of(scheduledOp), Optional.empty()))
        .thenReturn(false);

    runner.analyze(OFD_TYPE);

    verify(operationsRepo, never()).save(any());
  }

  // --- parallel database processing ---

  @Test
  void analyze_processesAllDatabases_inParallel() {
    AnalyzerRunner parallelRunner =
        new AnalyzerRunner(
            List.of(analyzer), statsRepo, operationsRepo, historyRepo, runStateRepo, 4);
    List<String> dbs = List.of("dbA", "dbB", "dbC");
    when(statsRepo.findDistinctDatabaseNames()).thenReturn(dbs);
    when(historyRepo.findLatest(eq(OFD_DB), any())).thenReturn(Collections.emptyList());
    for (String db : dbs) {
      TableStatsRow row =
          TableStatsRow.builder().tableUuid("uuid-" + db).databaseName(db).tableName("t").build();
      TableDto table = TableDto.fromRow(row);
      when(statsRepo.find(eq(Optional.of(db)), eq(Optional.empty()), eq(Optional.empty()), any()))
          .thenReturn(List.of(row));
      when(operationsRepo.find(
              eq(Optional.of(OFD_DB)),
              eq(Optional.empty()),
              eq(Optional.empty()),
              eq(Optional.of(db)),
              eq(Optional.empty()),
              eq(Optional.empty()),
              eq(Optional.empty()),
              any()))
          .thenReturn(Collections.emptyList());
      when(analyzer.isEnabled(table)).thenReturn(true);
      when(analyzer.shouldSchedule(eq(table), any(), any())).thenReturn(true);
    }

    parallelRunner.analyze(OFD_TYPE);

    // Every database was analyzed and produced its PENDING op.
    verify(operationsRepo, times(dbs.size())).save(any());
  }

  @Test
  void analyze_isolatesDatabaseFailure_othersStillProcessed() {
    AnalyzerRunner parallelRunner =
        new AnalyzerRunner(
            List.of(analyzer), statsRepo, operationsRepo, historyRepo, runStateRepo, 4);
    when(statsRepo.findDistinctDatabaseNames()).thenReturn(List.of("bad", "good"));
    when(historyRepo.findLatest(eq(OFD_DB), any())).thenReturn(Collections.emptyList());

    // "bad" fails on its first read query.
    when(operationsRepo.find(
            eq(Optional.of(OFD_DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.of("bad")),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenThrow(new RuntimeException("boom"));

    // "good" is processed normally.
    TableStatsRow good =
        TableStatsRow.builder().tableUuid("uuid-good").databaseName("good").tableName("t").build();
    TableDto goodTable = TableDto.fromRow(good);
    when(statsRepo.find(eq(Optional.of("good")), eq(Optional.empty()), eq(Optional.empty()), any()))
        .thenReturn(List.of(good));
    when(operationsRepo.find(
            eq(Optional.of(OFD_DB)),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.of("good")),
            eq(Optional.empty()),
            eq(Optional.empty()),
            eq(Optional.empty()),
            any()))
        .thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(goodTable)).thenReturn(true);
    when(analyzer.shouldSchedule(eq(goodTable), any(), any())).thenReturn(true);

    // The failing database is isolated: analyze() completes and the healthy database is processed.
    assertThatCode(() -> parallelRunner.analyze(OFD_TYPE)).doesNotThrowAnyException();
    verify(operationsRepo, times(1)).save(any());
  }

  // --- incremental scan ---

  @Test
  void analyzeIncremental_processesChangedTables_andAdvancesWatermark() {
    when(runStateRepo.findById("ORPHAN_FILES_DELETION")).thenReturn(Optional.empty());
    TableStatsRow changed =
        TableStatsRow.builder()
            .tableUuid("uuid-1")
            .databaseName(DB)
            .tableName("t")
            .updatedAt(Instant.now())
            .build();
    TableDto table = TableDto.fromRow(changed);
    // Join row: [TableStatsRow, currentOp(null), lastCompletedAt(null), lastStatus(null)]
    when(statsRepo.findChangedWithOpAndLatestHistory(eq(OFD_DB), any(), any()))
        .thenReturn(Collections.singletonList(new Object[] {changed, null, null, null}));
    when(analyzer.isEnabled(table)).thenReturn(true);
    when(analyzer.shouldSchedule(eq(table), any(), any())).thenReturn(true);

    runner.analyzeIncremental(OFD_TYPE);

    verify(operationsRepo, times(1)).save(any());
    ArgumentCaptor<AnalyzerRunStateRow> wm = ArgumentCaptor.forClass(AnalyzerRunStateRow.class);
    verify(runStateRepo).save(wm.capture());
    assertThat(wm.getValue().getOperationType()).isEqualTo("ORPHAN_FILES_DELETION");
    assertThat(wm.getValue().getWatermark()).isNotNull();
  }

  @Test
  void analyzeIncremental_advancesWatermark_whenNothingChanged() {
    when(runStateRepo.findById("ORPHAN_FILES_DELETION")).thenReturn(Optional.empty());
    when(statsRepo.findChangedWithOpAndLatestHistory(eq(OFD_DB), any(), any()))
        .thenReturn(Collections.emptyList());

    runner.analyzeIncremental(OFD_TYPE);

    verify(operationsRepo, never()).save(any());
    verify(runStateRepo, times(1)).save(any());
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

    runner.analyze(OFD_TYPE);

    verify(operationsRepo, never()).save(any());
  }
}
