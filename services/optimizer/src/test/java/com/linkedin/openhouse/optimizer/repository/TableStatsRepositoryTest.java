package com.linkedin.openhouse.optimizer.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.db.HistoryStatus;
import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.SnapshotMetrics;
import com.linkedin.openhouse.optimizer.db.TableOperationsHistoryRow;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
import com.linkedin.openhouse.optimizer.model.ChangedTableDto;
import com.linkedin.openhouse.optimizer.model.HistoryStatusDto;
import com.linkedin.openhouse.optimizer.model.OperationStatusDto;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class TableStatsRepositoryTest {

  private static final Pageable PAGE = PageRequest.of(0, 10_000);

  @Autowired TableStatsRepository repository;
  @Autowired TableOperationsRepository operationsRepository;
  @Autowired TableOperationsHistoryRepository historyRepository;

  @Test
  void saveAndFindById() {
    String tableUuid = UUID.randomUUID().toString();
    SnapshotMetrics snapshot = SnapshotMetrics.builder().tableSizeBytes(1024L).build();

    repository.save(
        TableStatsRow.builder()
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .snapshot(snapshot)
            .tableProperties(Map.of("maintenance.optimizer.ofd.enabled", "true"))
            .updatedAt(Instant.now())
            .build());

    Optional<TableStatsRow> found = repository.findById(tableUuid);
    assertThat(found).isPresent();
    assertThat(found.get().getDatabaseName()).isEqualTo("db1");
    assertThat(found.get().getSnapshot().getTableSizeBytes()).isEqualTo(1024L);
    assertThat(found.get().getTableProperties())
        .containsEntry("maintenance.optimizer.ofd.enabled", "true");
  }

  @Test
  void upsert_overwritesPreviousStats() {
    String tableUuid = UUID.randomUUID().toString();

    repository.save(
        TableStatsRow.builder()
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .snapshot(SnapshotMetrics.builder().tableSizeBytes(100L).build())
            .updatedAt(Instant.now())
            .build());

    repository.save(
        TableStatsRow.builder()
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .snapshot(SnapshotMetrics.builder().tableSizeBytes(200L).build())
            .updatedAt(Instant.now())
            .build());

    assertThat(repository.findAll()).hasSize(1);
    assertThat(repository.findById(tableUuid).get().getSnapshot().getTableSizeBytes())
        .isEqualTo(200L);
  }

  @Test
  void find_noParams_returnsAll() {
    repository.save(
        TableStatsRow.builder()
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl1")
            .snapshot(SnapshotMetrics.builder().tableSizeBytes(100L).build())
            .updatedAt(Instant.now())
            .build());
    repository.save(
        TableStatsRow.builder()
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db2")
            .tableName("tbl2")
            .snapshot(SnapshotMetrics.builder().tableSizeBytes(200L).build())
            .updatedAt(Instant.now())
            .build());

    assertThat(repository.find(Optional.empty(), Optional.empty(), Optional.empty(), PAGE))
        .hasSize(2);
  }

  @Test
  void find_byDatabase() {
    repository.save(
        TableStatsRow.builder()
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl1")
            .snapshot(SnapshotMetrics.builder().tableSizeBytes(100L).build())
            .updatedAt(Instant.now())
            .build());
    repository.save(
        TableStatsRow.builder()
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db2")
            .tableName("tbl2")
            .snapshot(SnapshotMetrics.builder().tableSizeBytes(200L).build())
            .updatedAt(Instant.now())
            .build());

    assertThat(repository.find(Optional.of("db1"), Optional.empty(), Optional.empty(), PAGE))
        .hasSize(1);
    assertThat(
            repository
                .find(Optional.of("db1"), Optional.empty(), Optional.empty(), PAGE)
                .get(0)
                .getDatabaseName())
        .isEqualTo("db1");
  }

  @Test
  void findChangedWithOpAndLatestHistory_joinsCurrentOpAndLatestHistory() {
    Instant watermark = Instant.parse("2026-01-01T00:00:00Z");
    String uuid = UUID.randomUUID().toString();
    repository.save(
        TableStatsRow.builder()
            .tableUuid(uuid)
            .databaseName("db1")
            .tableName("t")
            .updatedAt(watermark.plusSeconds(60))
            .build());
    // older row (before watermark) -> excluded from the changed set
    repository.save(
        TableStatsRow.builder()
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("old")
            .updatedAt(watermark.minusSeconds(60))
            .build());
    operationsRepository.save(
        TableOperationsRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(uuid)
            .databaseName("db1")
            .tableName("t")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(OperationStatus.PENDING)
            .createdAt(Instant.now())
            .build());
    Instant t1 = Instant.parse("2025-12-01T00:00:00Z");
    Instant t2 = Instant.parse("2025-12-15T00:00:00Z");
    historyRepository.save(historyRow(uuid, t1, HistoryStatus.SUCCESS));
    historyRepository.save(historyRow(uuid, t2, HistoryStatus.FAILED)); // latest

    List<ChangedTableDto> changed =
        repository
            .findChangedWithOpAndLatestHistory(OperationType.ORPHAN_FILES_DELETION, watermark, PAGE)
            .stream()
            .map(ChangedTableDto::fromJoinRow)
            .collect(Collectors.toList());

    assertThat(changed).hasSize(1);
    ChangedTableDto c = changed.get(0);
    assertThat(c.getTable().getTableUuid()).isEqualTo(uuid);
    assertThat(c.currentOp()).isPresent();
    assertThat(c.currentOp().get().getStatus()).isEqualTo(OperationStatusDto.PENDING);
    assertThat(c.latestHistory()).isPresent();
    assertThat(c.latestHistory().get().getCompletedAt()).isEqualTo(t2);
    assertThat(c.latestHistory().get().getStatus()).isEqualTo(HistoryStatusDto.FAILED);
  }

  private static TableOperationsHistoryRow historyRow(
      String uuid, Instant completedAt, HistoryStatus status) {
    return TableOperationsHistoryRow.builder()
        .id(UUID.randomUUID().toString())
        .tableUuid(uuid)
        .databaseName("db1")
        .tableName("t")
        .operationType(OperationType.ORPHAN_FILES_DELETION)
        .completedAt(completedAt)
        .status(status)
        .build();
  }
}
