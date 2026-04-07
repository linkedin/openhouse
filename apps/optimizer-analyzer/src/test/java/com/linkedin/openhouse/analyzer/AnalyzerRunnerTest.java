package com.linkedin.openhouse.analyzer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.analyzer.model.TableOperationRecord;
import com.linkedin.openhouse.analyzer.model.TableSummary;
import com.linkedin.openhouse.optimizer.entity.TableOperationHistoryRow;
import com.linkedin.openhouse.optimizer.entity.TableOperationRow;
import com.linkedin.openhouse.optimizer.entity.TableStatsRow;
import com.linkedin.openhouse.optimizer.repository.TableOperationHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;

@ExtendWith(MockitoExtension.class)
class AnalyzerRunnerTest {

  @Mock private TableStatsRepository statsRepo;
  @Mock private TableOperationsRepository operationsRepo;
  @Mock private TableOperationHistoryRepository historyRepo;
  @Mock private OperationAnalyzer analyzer;

  private AnalyzerRunner runner;

  @BeforeEach
  void setUp() {
    runner = new AnalyzerRunner(List.of(analyzer), statsRepo, operationsRepo, historyRepo);
  }

  @Test
  void analyze_insertsNewRow_forEligibleTableWithNoExistingOp() {
    TableStatsRow statsEntity = new TableStatsRow();
    statsEntity.setTableUuid("uuid-1");
    statsEntity.setDatabaseId("db1");
    statsEntity.setTableName("tbl1");

    TableSummary expectedTable =
        TableSummary.builder().tableUuid("uuid-1").databaseId("db1").tableId("tbl1").build();

    when(statsRepo.find(null, null, null)).thenReturn(List.of(statsEntity));
    when(analyzer.getOperationType()).thenReturn("ORPHAN_FILES_DELETION");
    when(analyzer.getCircuitBreakerThreshold()).thenReturn(5);
    when(operationsRepo.find("ORPHAN_FILES_DELETION", null, null, null, null))
        .thenReturn(Collections.emptyList());
    when(historyRepo.find("ORPHAN_FILES_DELETION", null, null, null, Pageable.unpaged()))
        .thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);
    when(analyzer.shouldSchedule(expectedTable, Optional.empty(), Optional.empty()))
        .thenReturn(true);

    runner.analyze();

    ArgumentCaptor<TableOperationRow> captor = ArgumentCaptor.forClass(TableOperationRow.class);
    verify(operationsRepo).save(captor.capture());
    TableOperationRow saved = captor.getValue();
    assertThat(saved.getTableUuid()).isEqualTo("uuid-1");
    assertThat(saved.getDatabaseName()).isEqualTo("db1");
    assertThat(saved.getTableName()).isEqualTo("tbl1");
    assertThat(saved.getOperationType()).isEqualTo("ORPHAN_FILES_DELETION");
    assertThat(saved.getStatus()).isEqualTo("PENDING");
    assertThat(saved.getId()).isNotNull();
  }

  @Test
  void analyze_noOp_whenCadencePolicyReturnsFalseForPending() {
    TableStatsRow statsEntity = new TableStatsRow();
    statsEntity.setTableUuid("uuid-1");
    statsEntity.setDatabaseId("db1");
    statsEntity.setTableName("tbl1");

    TableSummary expectedTable =
        TableSummary.builder().tableUuid("uuid-1").databaseId("db1").tableId("tbl1").build();

    TableOperationRow existingEntity = new TableOperationRow();
    existingEntity.setId("existing-op-id");
    existingEntity.setStatus("PENDING");
    existingEntity.setTableUuid("uuid-1");
    existingEntity.setOperationType("ORPHAN_FILES_DELETION");
    existingEntity.setCreatedAt(Instant.now());

    when(statsRepo.find(null, null, null)).thenReturn(List.of(statsEntity));
    when(analyzer.getOperationType()).thenReturn("ORPHAN_FILES_DELETION");
    when(operationsRepo.find("ORPHAN_FILES_DELETION", null, null, null, null))
        .thenReturn(List.of(existingEntity));
    when(historyRepo.find("ORPHAN_FILES_DELETION", null, null, null, Pageable.unpaged()))
        .thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);

    TableOperationRecord existingRecord = new TableOperationRecord();
    existingRecord.setId("existing-op-id");
    existingRecord.setStatus("PENDING");
    existingRecord.setTableUuid("uuid-1");
    existingRecord.setOperationType("ORPHAN_FILES_DELETION");
    existingRecord.setCreatedAt(existingEntity.getCreatedAt());
    when(analyzer.shouldSchedule(expectedTable, Optional.of(existingRecord), Optional.empty()))
        .thenReturn(false);

    runner.analyze();

    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_skipsTable_whenNotEnabled() {
    TableStatsRow statsEntity = new TableStatsRow();
    statsEntity.setTableUuid("uuid-1");

    TableSummary expectedTable = TableSummary.builder().tableUuid("uuid-1").build();

    when(statsRepo.find(null, null, null)).thenReturn(List.of(statsEntity));
    when(analyzer.getOperationType()).thenReturn("ORPHAN_FILES_DELETION");
    when(operationsRepo.find("ORPHAN_FILES_DELETION", null, null, null, null))
        .thenReturn(Collections.emptyList());
    when(historyRepo.find("ORPHAN_FILES_DELETION", null, null, null, Pageable.unpaged()))
        .thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(expectedTable)).thenReturn(false);

    runner.analyze();

    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_skipsTable_whenShouldScheduleReturnsFalse() {
    TableStatsRow statsEntity = new TableStatsRow();
    statsEntity.setTableUuid("uuid-1");

    TableSummary expectedTable = TableSummary.builder().tableUuid("uuid-1").build();

    TableOperationRow scheduled = new TableOperationRow();
    scheduled.setId("op-id");
    scheduled.setStatus("SCHEDULED");
    scheduled.setTableUuid("uuid-1");
    scheduled.setOperationType("ORPHAN_FILES_DELETION");
    scheduled.setCreatedAt(Instant.now());

    when(statsRepo.find(null, null, null)).thenReturn(List.of(statsEntity));
    when(analyzer.getOperationType()).thenReturn("ORPHAN_FILES_DELETION");
    when(operationsRepo.find("ORPHAN_FILES_DELETION", null, null, null, null))
        .thenReturn(List.of(scheduled));
    when(historyRepo.find("ORPHAN_FILES_DELETION", null, null, null, Pageable.unpaged()))
        .thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);

    TableOperationRecord scheduledRecord = new TableOperationRecord();
    scheduledRecord.setId("op-id");
    scheduledRecord.setStatus("SCHEDULED");
    scheduledRecord.setTableUuid("uuid-1");
    scheduledRecord.setOperationType("ORPHAN_FILES_DELETION");
    scheduledRecord.setCreatedAt(scheduled.getCreatedAt());
    when(analyzer.shouldSchedule(expectedTable, Optional.of(scheduledRecord), Optional.empty()))
        .thenReturn(false);

    runner.analyze();

    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_skipsTable_whenTableUuidIsNull() {
    TableStatsRow statsEntity = new TableStatsRow();
    statsEntity.setTableUuid(null);

    when(statsRepo.find(null, null, null)).thenReturn(List.of(statsEntity));
    when(analyzer.getOperationType()).thenReturn("ORPHAN_FILES_DELETION");
    when(operationsRepo.find("ORPHAN_FILES_DELETION", null, null, null, null))
        .thenReturn(Collections.emptyList());
    when(historyRepo.find(anyString(), any(), any(), any(), any()))
        .thenReturn(Collections.emptyList());

    runner.analyze();

    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_skipsTable_whenCircuitBreakerTrips() {
    TableStatsRow statsEntity = new TableStatsRow();
    statsEntity.setTableUuid("uuid-1");
    statsEntity.setDatabaseId("db1");
    statsEntity.setTableName("tbl1");

    TableSummary expectedTable =
        TableSummary.builder().tableUuid("uuid-1").databaseId("db1").tableId("tbl1").build();

    List<TableOperationHistoryRow> failures =
        IntStream.range(0, 3)
            .mapToObj(
                i ->
                    TableOperationHistoryRow.builder()
                        .id("fail-" + i)
                        .tableUuid("uuid-1")
                        .operationType("ORPHAN_FILES_DELETION")
                        .submittedAt(Instant.now().minusSeconds(i * 60))
                        .status("FAILED")
                        .build())
            .collect(Collectors.toList());

    when(statsRepo.find(null, null, null)).thenReturn(List.of(statsEntity));
    when(analyzer.getOperationType()).thenReturn("ORPHAN_FILES_DELETION");
    when(analyzer.getCircuitBreakerThreshold()).thenReturn(3);
    when(operationsRepo.find("ORPHAN_FILES_DELETION", null, null, null, null))
        .thenReturn(Collections.emptyList());
    when(historyRepo.find("ORPHAN_FILES_DELETION", null, null, null, Pageable.unpaged()))
        .thenReturn(failures);
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);
    when(analyzer.shouldSchedule(expectedTable, Optional.empty(), Optional.of(failures.get(0))))
        .thenReturn(true);

    runner.analyze();

    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_doesNotTrip_whenFewerFailuresThanThreshold() {
    TableStatsRow statsEntity = new TableStatsRow();
    statsEntity.setTableUuid("uuid-1");
    statsEntity.setDatabaseId("db1");
    statsEntity.setTableName("tbl1");

    TableSummary expectedTable =
        TableSummary.builder().tableUuid("uuid-1").databaseId("db1").tableId("tbl1").build();

    List<TableOperationHistoryRow> failures =
        IntStream.range(0, 3)
            .mapToObj(
                i ->
                    TableOperationHistoryRow.builder()
                        .id("fail-" + i)
                        .tableUuid("uuid-1")
                        .operationType("ORPHAN_FILES_DELETION")
                        .submittedAt(Instant.now().minusSeconds(i * 60))
                        .status("FAILED")
                        .build())
            .collect(Collectors.toList());

    when(statsRepo.find(null, null, null)).thenReturn(List.of(statsEntity));
    when(analyzer.getOperationType()).thenReturn("ORPHAN_FILES_DELETION");
    when(analyzer.getCircuitBreakerThreshold()).thenReturn(5);
    when(operationsRepo.find("ORPHAN_FILES_DELETION", null, null, null, null))
        .thenReturn(Collections.emptyList());
    when(historyRepo.find("ORPHAN_FILES_DELETION", null, null, null, Pageable.unpaged()))
        .thenReturn(failures);
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);
    when(analyzer.shouldSchedule(expectedTable, Optional.empty(), Optional.of(failures.get(0))))
        .thenReturn(true);

    runner.analyze();

    verify(operationsRepo).save(any());
  }
}
