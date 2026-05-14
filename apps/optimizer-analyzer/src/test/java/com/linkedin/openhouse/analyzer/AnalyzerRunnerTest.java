package com.linkedin.openhouse.analyzer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.optimizer.entity.TableOperationsRow;
import com.linkedin.openhouse.optimizer.entity.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.Table;
import com.linkedin.openhouse.optimizer.model.TableOperation;
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

  private static final OperationType OFD_TYPE = OperationType.ORPHAN_FILES_DELETION;
  private static final String OFD = OFD_TYPE.name();
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
    TableStatsRow statsEntity = new TableStatsRow();
    statsEntity.setTableUuid("uuid-1");
    statsEntity.setDatabaseName(DB);
    statsEntity.setTableName("tbl1");

    Table expectedTable =
        Table.builder().tableUuid("uuid-1").databaseName(DB).tableId("tbl1").build();

    when(statsRepo.find(DB, null, null)).thenReturn(List.of(statsEntity));
    when(operationsRepo.find(OFD, null, null, DB, null)).thenReturn(Collections.emptyList());
    when(historyRepo.findLatestPerTable(OFD)).thenReturn(Collections.emptyList());
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
    assertThat(saved.getOperationType()).isEqualTo(OFD);
    assertThat(saved.getStatus()).isEqualTo("PENDING");
    assertThat(saved.getId()).isNotNull();
  }

  @Test
  void analyze_noOp_whenCadencePolicyReturnsFalseForPending() {
    TableStatsRow statsEntity = new TableStatsRow();
    statsEntity.setTableUuid("uuid-1");
    statsEntity.setDatabaseName(DB);
    statsEntity.setTableName("tbl1");

    Table expectedTable =
        Table.builder().tableUuid("uuid-1").databaseName(DB).tableId("tbl1").build();

    TableOperationsRow existingEntity = new TableOperationsRow();
    existingEntity.setId("existing-op-id");
    existingEntity.setStatus("PENDING");
    existingEntity.setTableUuid("uuid-1");
    existingEntity.setOperationType(OFD);
    existingEntity.setCreatedAt(Instant.now());

    when(statsRepo.find(DB, null, null)).thenReturn(List.of(statsEntity));
    when(operationsRepo.find(OFD, null, null, DB, null)).thenReturn(List.of(existingEntity));
    when(historyRepo.findLatestPerTable(OFD)).thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);

    TableOperation existingOp = TableOperation.from(existingEntity);
    when(analyzer.shouldSchedule(expectedTable, Optional.of(existingOp), Optional.empty()))
        .thenReturn(false);

    runner.analyze(OFD_TYPE);

    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_skipsTable_whenNotEnabled() {
    TableStatsRow statsEntity = new TableStatsRow();
    statsEntity.setTableUuid("uuid-1");
    statsEntity.setDatabaseName(DB);

    Table expectedTable = Table.builder().tableUuid("uuid-1").databaseName(DB).build();

    when(statsRepo.find(DB, null, null)).thenReturn(List.of(statsEntity));
    when(operationsRepo.find(OFD, null, null, DB, null)).thenReturn(Collections.emptyList());
    when(historyRepo.findLatestPerTable(OFD)).thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(expectedTable)).thenReturn(false);

    runner.analyze(OFD_TYPE);

    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_skipsTable_whenShouldScheduleReturnsFalse() {
    TableStatsRow statsEntity = new TableStatsRow();
    statsEntity.setTableUuid("uuid-1");
    statsEntity.setDatabaseName(DB);

    Table expectedTable = Table.builder().tableUuid("uuid-1").databaseName(DB).build();

    TableOperationsRow scheduled = new TableOperationsRow();
    scheduled.setId("op-id");
    scheduled.setStatus("SCHEDULED");
    scheduled.setTableUuid("uuid-1");
    scheduled.setOperationType(OFD);
    scheduled.setCreatedAt(Instant.now());

    when(statsRepo.find(DB, null, null)).thenReturn(List.of(statsEntity));
    when(operationsRepo.find(OFD, null, null, DB, null)).thenReturn(List.of(scheduled));
    when(historyRepo.findLatestPerTable(OFD)).thenReturn(Collections.emptyList());
    when(analyzer.isEnabled(expectedTable)).thenReturn(true);

    TableOperation scheduledOp = TableOperation.from(scheduled);
    when(analyzer.shouldSchedule(expectedTable, Optional.of(scheduledOp), Optional.empty()))
        .thenReturn(false);

    runner.analyze(OFD_TYPE);

    verify(operationsRepo, never()).save(any());
  }

  @Test
  void analyze_skipsTable_whenTableUuidIsNull() {
    TableStatsRow statsEntity = new TableStatsRow();
    statsEntity.setTableUuid(null);
    statsEntity.setDatabaseName(DB);

    when(statsRepo.find(DB, null, null)).thenReturn(List.of(statsEntity));
    when(operationsRepo.find(OFD, null, null, DB, null)).thenReturn(Collections.emptyList());
    when(historyRepo.findLatestPerTable(anyString())).thenReturn(Collections.emptyList());

    runner.analyze(OFD_TYPE);

    verify(operationsRepo, never()).save(any());
  }
}
