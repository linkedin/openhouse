package com.linkedin.openhouse.analyzer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    when(analyzer.getOperationType()).thenReturn(OFD_TYPE);
    when(statsRepo.findDistinctDatabaseNames()).thenReturn(List.of(DB));
  }

  @Test
  void analyze_insertsNewRow_forEligibleTableWithNoExistingOp() {
    TableStatsRow statsEntity =
        TableStatsRow.builder().tableUuid("uuid-1").databaseName(DB).tableName("tbl1").build();

    TableDto expectedTable = TableDto.fromRow(statsEntity);

    when(statsRepo.find(DB, null, null)).thenReturn(List.of(statsEntity));
    when(operationsRepo.find(OFD_DB, null, null, DB, null)).thenReturn(Collections.emptyList());
    when(historyRepo.findLatestPerTable(OFD_DB)).thenReturn(Collections.emptyList());
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

    when(statsRepo.find(DB, null, null)).thenReturn(List.of(statsEntity));
    when(operationsRepo.find(OFD_DB, null, null, DB, null)).thenReturn(List.of(existingEntity));
    when(historyRepo.findLatestPerTable(OFD_DB)).thenReturn(Collections.emptyList());
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

    when(statsRepo.find(DB, null, null)).thenReturn(List.of(statsEntity));
    when(operationsRepo.find(OFD_DB, null, null, DB, null)).thenReturn(Collections.emptyList());
    when(historyRepo.findLatestPerTable(OFD_DB)).thenReturn(Collections.emptyList());
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

    when(statsRepo.find(DB, null, null)).thenReturn(List.of(statsEntity));
    when(operationsRepo.find(OFD_DB, null, null, DB, null)).thenReturn(List.of(scheduled));
    when(historyRepo.findLatestPerTable(OFD_DB)).thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);

    TableOperationDto scheduledOp = TableOperationDto.fromRow(scheduled);
    when(analyzer.shouldSchedule(expectedTable, Optional.of(scheduledOp), Optional.empty()))
        .thenReturn(false);

    runner.analyze(OFD_TYPE);

    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_skipsTable_whenTableUuidIsNull() {
    TableStatsRow statsEntity = TableStatsRow.builder().databaseName(DB).build();

    when(statsRepo.find(DB, null, null)).thenReturn(List.of(statsEntity));
    when(operationsRepo.find(OFD_DB, null, null, DB, null)).thenReturn(Collections.emptyList());
    when(historyRepo.findLatestPerTable(any())).thenReturn(Collections.emptyList());

    runner.analyze(OFD_TYPE);

    verify(operationsRepo, never()).save(any());
  }
}
