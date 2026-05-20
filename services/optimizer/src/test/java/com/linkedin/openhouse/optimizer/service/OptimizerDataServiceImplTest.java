package com.linkedin.openhouse.optimizer.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsHistoryRow;
import com.linkedin.openhouse.optimizer.model.HistoryStatus;
import com.linkedin.openhouse.optimizer.model.OperationStatus;
import com.linkedin.openhouse.optimizer.model.OperationType;
import com.linkedin.openhouse.optimizer.model.TableOperationsHistory;
import com.linkedin.openhouse.optimizer.model.TableStats;
import com.linkedin.openhouse.optimizer.repository.TableOperationsRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsHistoryRepository;
import com.linkedin.openhouse.optimizer.repository.TableStatsRepository;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class OptimizerDataServiceImplTest {

  @Autowired OptimizerDataService service;
  @Autowired TableOperationsRepository operationsRepository;
  @Autowired TableStatsRepository statsRepository;
  @Autowired TableStatsHistoryRepository statsHistoryRepository;

  // --- completeOperation ---

  @Test
  void completeOperation_writesHistoryFromOperationRow() {
    String operationId = UUID.randomUUID().toString();
    String tableUuid = UUID.randomUUID().toString();
    operationsRepository.save(
        TableOperationsRow.builder()
            .id(operationId)
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(com.linkedin.openhouse.optimizer.db.OperationType.ORPHAN_FILES_DELETION)
            .status(com.linkedin.openhouse.optimizer.db.OperationStatus.SCHEDULED)
            .createdAt(Instant.now())
            .scheduledAt(Instant.now())
            .jobId("spark-job-123")
            .build());

    Optional<TableOperationsHistory> result =
        service.completeOperation(operationId, HistoryStatus.SUCCESS, 42L, 1024L, null, null);

    assertThat(result).isPresent();
    assertThat(result.get().getStatus()).isEqualTo(HistoryStatus.SUCCESS);
    assertThat(result.get().getTableUuid()).isEqualTo(tableUuid);
    assertThat(result.get().getOperationType()).isEqualTo(OperationType.ORPHAN_FILES_DELETION);
    assertThat(result.get().getDatabaseName()).isEqualTo("db1");
    assertThat(result.get().getCompletedAt()).isNotNull();
    assertThat(result.get().getOrphanFilesDeleted()).isEqualTo(42L);
    assertThat(result.get().getOrphanBytesDeleted()).isEqualTo(1024L);
    assertThat(result.get().getErrorMessage()).isNull();
    assertThat(result.get().getErrorType()).isNull();
  }

  @Test
  void completeOperation_failurePersistsErrorFields() {
    String operationId = UUID.randomUUID().toString();
    operationsRepository.save(
        TableOperationsRow.builder()
            .id(operationId)
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(com.linkedin.openhouse.optimizer.db.OperationType.ORPHAN_FILES_DELETION)
            .status(com.linkedin.openhouse.optimizer.db.OperationStatus.SCHEDULED)
            .createdAt(Instant.now())
            .scheduledAt(Instant.now())
            .jobId("spark-job-456")
            .build());

    Optional<TableOperationsHistory> result =
        service.completeOperation(
            operationId, HistoryStatus.FAILED, null, null, "boom", "RuntimeException");

    assertThat(result).isPresent();
    assertThat(result.get().getStatus()).isEqualTo(HistoryStatus.FAILED);
    assertThat(result.get().getOrphanFilesDeleted()).isNull();
    assertThat(result.get().getOrphanBytesDeleted()).isNull();
    assertThat(result.get().getErrorMessage()).isEqualTo("boom");
    assertThat(result.get().getErrorType()).isEqualTo("RuntimeException");
  }

  @Test
  void completeOperation_notFound_returnsEmpty() {
    Optional<TableOperationsHistory> result =
        service.completeOperation(
            UUID.randomUUID().toString(), HistoryStatus.FAILED, null, null, null, null);

    assertThat(result).isEmpty();
  }

  // --- upsertTableStats ---

  @Test
  void upsertTableStats_createsNewRow() {
    String tableUuid = UUID.randomUUID().toString();
    TableStats input =
        TableStats.builder()
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .tableProperties(Map.of("maintenance.optimizer.ofd.enabled", "true"))
            .snapshot(TableStats.SnapshotMetrics.builder().tableSizeBytes(1024L).build())
            .build();

    TableStats result = service.upsertTableStats(input);

    assertThat(result.getTableUuid()).isEqualTo(tableUuid);
    assertThat(result.getDatabaseName()).isEqualTo("db1");
    assertThat(result.getSnapshot().getTableSizeBytes()).isEqualTo(1024L);
    assertThat(result.getTableProperties())
        .containsEntry("maintenance.optimizer.ofd.enabled", "true");
    assertThat(result.getUpdatedAt()).isNotNull();
    assertThat(statsRepository.findById(tableUuid)).isPresent();
  }

  @Test
  void upsertTableStats_updatesExistingRow_andAppendsHistory() {
    String tableUuid = UUID.randomUUID().toString();
    TableStats first =
        TableStats.builder()
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .snapshot(TableStats.SnapshotMetrics.builder().tableSizeBytes(100L).build())
            .delta(TableStats.CommitDelta.builder().numFilesAdded(5L).numFilesDeleted(1L).build())
            .build();
    TableStats second =
        TableStats.builder()
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .snapshot(TableStats.SnapshotMetrics.builder().tableSizeBytes(200L).build())
            .delta(TableStats.CommitDelta.builder().numFilesAdded(3L).numFilesDeleted(0L).build())
            .build();

    service.upsertTableStats(first);
    TableStats result = service.upsertTableStats(second);

    assertThat(result.getSnapshot().getTableSizeBytes()).isEqualTo(200L);
    assertThat(statsRepository.findAll()).hasSize(1);

    List<TableStatsHistoryRow> history =
        statsHistoryRepository.find(tableUuid, null, PageRequest.of(0, 100));
    assertThat(history).hasSize(2);
    assertThat(history.get(0).getDelta().getNumFilesAdded()).isEqualTo(3L);
    assertThat(history.get(1).getDelta().getNumFilesAdded()).isEqualTo(5L);
  }

  // --- list filters touch the operations enum mapping path ---

  @Test
  void listTableOperations_filtersByOperationTypeAndStatus() {
    String pendingId = UUID.randomUUID().toString();
    String scheduledId = UUID.randomUUID().toString();
    operationsRepository.save(
        TableOperationsRow.builder()
            .id(pendingId)
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(com.linkedin.openhouse.optimizer.db.OperationType.ORPHAN_FILES_DELETION)
            .status(com.linkedin.openhouse.optimizer.db.OperationStatus.PENDING)
            .createdAt(Instant.now())
            .build());
    operationsRepository.save(
        TableOperationsRow.builder()
            .id(scheduledId)
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl2")
            .operationType(com.linkedin.openhouse.optimizer.db.OperationType.ORPHAN_FILES_DELETION)
            .status(com.linkedin.openhouse.optimizer.db.OperationStatus.SCHEDULED)
            .createdAt(Instant.now())
            .build());

    assertThat(
            service.listTableOperations(
                Optional.of(OperationType.ORPHAN_FILES_DELETION),
                Optional.of(OperationStatus.PENDING),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()))
        .extracting(op -> op.getId())
        .containsExactly(pendingId);
  }
}
