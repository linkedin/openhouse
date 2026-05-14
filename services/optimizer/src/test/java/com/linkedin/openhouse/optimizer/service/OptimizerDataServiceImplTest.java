package com.linkedin.openhouse.optimizer.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.api.model.CompleteOperationRequest;
import com.linkedin.openhouse.optimizer.api.model.HistoryStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.api.model.TableStats;
import com.linkedin.openhouse.optimizer.api.model.TableStatsDto;
import com.linkedin.openhouse.optimizer.api.model.UpsertTableStatsRequest;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsHistoryRow;
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

    Optional<TableOperationsHistoryDto> result =
        service.completeOperation(
            CompleteOperationRequest.builder()
                .operationId(operationId)
                .status(HistoryStatus.SUCCESS)
                .build());

    assertThat(result).isPresent();
    assertThat(result.get().getStatus()).isEqualTo(HistoryStatus.SUCCESS);
    assertThat(result.get().getTableUuid()).isEqualTo(tableUuid);
    assertThat(result.get().getOperationType()).isEqualTo(OperationType.ORPHAN_FILES_DELETION);
    assertThat(result.get().getDatabaseName()).isEqualTo("db1");
    assertThat(result.get().getCompletedAt()).isNotNull();
  }

  @Test
  void completeOperation_notFound_returnsEmpty() {
    Optional<TableOperationsHistoryDto> result =
        service.completeOperation(
            CompleteOperationRequest.builder()
                .operationId(UUID.randomUUID().toString())
                .status(HistoryStatus.FAILED)
                .build());

    assertThat(result).isEmpty();
  }

  // --- upsertTableStats ---

  @Test
  void upsertTableStats_createsNewRow() {
    String tableUuid = UUID.randomUUID().toString();
    TableStats stats =
        TableStats.builder()
            .snapshot(TableStats.SnapshotMetrics.builder().tableSizeBytes(1024L).build())
            .build();

    TableStatsDto dto =
        service.upsertTableStats(
            tableUuid,
            UpsertTableStatsRequest.builder()
                .databaseName("db1")
                .tableName("tbl1")
                .stats(stats)
                .tableProperties(Map.of("maintenance.optimizer.ofd.enabled", "true"))
                .build());

    assertThat(dto.getTableUuid()).isEqualTo(tableUuid);
    assertThat(dto.getDatabaseName()).isEqualTo("db1");
    assertThat(dto.getStats().getSnapshot().getTableSizeBytes()).isEqualTo(1024L);
    assertThat(dto.getTableProperties()).containsEntry("maintenance.optimizer.ofd.enabled", "true");
    assertThat(statsRepository.findById(tableUuid)).isPresent();
  }

  @Test
  void upsertTableStats_updatesExistingRow_andAppendsHistory() {
    String tableUuid = UUID.randomUUID().toString();
    TableStats firstStats =
        TableStats.builder()
            .snapshot(TableStats.SnapshotMetrics.builder().tableSizeBytes(100L).build())
            .delta(TableStats.CommitDelta.builder().numFilesAdded(5L).numFilesDeleted(1L).build())
            .build();
    TableStats secondStats =
        TableStats.builder()
            .snapshot(TableStats.SnapshotMetrics.builder().tableSizeBytes(200L).build())
            .delta(TableStats.CommitDelta.builder().numFilesAdded(3L).numFilesDeleted(0L).build())
            .build();

    service.upsertTableStats(
        tableUuid,
        UpsertTableStatsRequest.builder()
            .databaseName("db1")
            .tableName("tbl1")
            .stats(firstStats)
            .build());
    TableStatsDto dto =
        service.upsertTableStats(
            tableUuid,
            UpsertTableStatsRequest.builder()
                .databaseName("db1")
                .tableName("tbl1")
                .stats(secondStats)
                .build());

    // Current row reflects the latest upsert's snapshot.
    assertThat(dto.getStats().getSnapshot().getTableSizeBytes()).isEqualTo(200L);
    assertThat(statsRepository.findAll()).hasSize(1);

    // History has one row per upsert with the raw delta from each call.
    List<TableStatsHistoryRow> history =
        statsHistoryRepository.find(tableUuid, null, PageRequest.of(0, 100));
    assertThat(history).hasSize(2);
    // Newest first.
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
        .extracting(dto -> dto.getId())
        .containsExactly(pendingId);
  }
}
