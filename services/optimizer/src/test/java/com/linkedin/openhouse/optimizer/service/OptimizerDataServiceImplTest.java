package com.linkedin.openhouse.optimizer.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.api.model.CompleteOperationRequest;
import com.linkedin.openhouse.optimizer.api.model.JobResult;
import com.linkedin.openhouse.optimizer.api.model.OperationHistoryStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.api.model.TableStats;
import com.linkedin.openhouse.optimizer.api.model.TableStatsDto;
import com.linkedin.openhouse.optimizer.api.model.UpsertTableStatsRequest;
import com.linkedin.openhouse.optimizer.entity.TableOperationsRow;
import com.linkedin.openhouse.optimizer.entity.TableStatsHistoryRow;
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
    String id = UUID.randomUUID().toString();
    String tableUuid = UUID.randomUUID().toString();
    operationsRepository.save(
        TableOperationsRow.builder()
            .id(id)
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(OperationStatus.SCHEDULED)
            .createdAt(Instant.now())
            .scheduledAt(Instant.now())
            .jobId("spark-job-123")
            .build());

    Optional<TableOperationsHistoryDto> result =
        service.completeOperation(
            id, CompleteOperationRequest.builder().status(OperationHistoryStatus.SUCCESS).build());

    assertThat(result).isPresent();
    assertThat(result.get().getStatus()).isEqualTo(OperationHistoryStatus.SUCCESS);
    assertThat(result.get().getTableUuid()).isEqualTo(tableUuid);
    assertThat(result.get().getJobId()).isEqualTo("spark-job-123");
    assertThat(result.get().getOperationType()).isEqualTo(OperationType.ORPHAN_FILES_DELETION);
    assertThat(result.get().getDatabaseName()).isEqualTo("db1");
    assertThat(result.get().getSubmittedAt()).isNotNull();
  }

  @Test
  void completeOperation_notFound_returnsEmpty() {
    Optional<TableOperationsHistoryDto> result =
        service.completeOperation(
            UUID.randomUUID().toString(),
            CompleteOperationRequest.builder()
                .status(OperationHistoryStatus.FAILED)
                .result(
                    JobResult.builder().errorMessage("boom").errorType("RuntimeException").build())
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
                .databaseId("db1")
                .tableName("tbl1")
                .stats(stats)
                .tableProperties(Map.of("maintenance.optimizer.ofd.enabled", "true"))
                .build());

    assertThat(dto.getTableUuid()).isEqualTo(tableUuid);
    assertThat(dto.getDatabaseId()).isEqualTo("db1");
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
            .databaseId("db1")
            .tableName("tbl1")
            .stats(firstStats)
            .build());
    TableStatsDto dto =
        service.upsertTableStats(
            tableUuid,
            UpsertTableStatsRequest.builder()
                .databaseId("db1")
                .tableName("tbl1")
                .stats(secondStats)
                .build());

    // Current row reflects the latest upsert
    assertThat(dto.getStats().getSnapshot().getTableSizeBytes()).isEqualTo(200L);
    assertThat(statsRepository.findAll()).hasSize(1);

    // History has one row per upsert with the raw delta from each call
    List<TableStatsHistoryRow> history =
        statsHistoryRepository.find(tableUuid, null, PageRequest.of(0, 100));
    assertThat(history).hasSize(2);
    // Newest first
    assertThat(history.get(0).getStats().getDelta().getNumFilesAdded()).isEqualTo(3L);
    assertThat(history.get(1).getStats().getDelta().getNumFilesAdded()).isEqualTo(5L);
  }
}
