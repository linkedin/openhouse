package com.linkedin.openhouse.optimizer.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.api.model.HistoryStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
import com.linkedin.openhouse.optimizer.entity.TableOperationsHistoryRow;
import java.time.Instant;
import java.util.List;
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
class TableOperationsHistoryRepositoryTest {

  @Autowired TableOperationsHistoryRepository repository;

  @Test
  void appendAndFindByTableUuid() {
    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");
    Instant t2 = Instant.parse("2024-01-02T10:00:00Z");
    String tableUuid = UUID.randomUUID().toString();

    repository.save(
        TableOperationsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION.name())
            .completedAt(t1)
            .status(HistoryStatus.SUCCESS.name())
            .jobId("job-001")
            .build());

    repository.save(
        TableOperationsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION.name())
            .completedAt(t2)
            .status(HistoryStatus.FAILED.name())
            .jobId("job-002")
            .result("{\"errorMessage\":\"out of memory\",\"errorType\":\"OOM\"}")
            .build());

    List<TableOperationsHistoryRow> rows =
        repository.find(null, null, tableUuid, null, null, null, null, PageRequest.of(0, 10));

    assertThat(rows).hasSize(2);
    // Newest first
    assertThat(rows.get(0).getJobId()).isEqualTo("job-002");
    assertThat(rows.get(1).getJobId()).isEqualTo("job-001");
  }

  @Test
  void appendIsNonDestructive_multipleRunsRetained() {
    Instant now = Instant.now();
    String tableUuid = UUID.randomUUID().toString();
    for (int i = 0; i < 3; i++) {
      repository.save(
          TableOperationsHistoryRow.builder()
              .id(UUID.randomUUID().toString())
              .tableUuid(tableUuid)
              .databaseName("db1")
              .tableName("tbl2")
              .operationType(OperationType.ORPHAN_FILES_DELETION.name())
              .completedAt(now.plusSeconds(i))
              .status(HistoryStatus.SUCCESS.name())
              .build());
    }

    List<TableOperationsHistoryRow> rows =
        repository.find(null, null, tableUuid, null, null, null, null, PageRequest.of(0, 10));
    assertThat(rows).hasSize(3);
  }

  @Test
  void find_respectsLimit() {
    Instant now = Instant.now();
    String tableUuid = UUID.randomUUID().toString();
    for (int i = 0; i < 5; i++) {
      repository.save(
          TableOperationsHistoryRow.builder()
              .id(UUID.randomUUID().toString())
              .tableUuid(tableUuid)
              .databaseName("db1")
              .tableName("tbl3")
              .operationType(OperationType.ORPHAN_FILES_DELETION.name())
              .completedAt(now.plusSeconds(i))
              .status(HistoryStatus.SUCCESS.name())
              .build());
    }

    List<TableOperationsHistoryRow> rows =
        repository.find(null, null, tableUuid, null, null, null, null, PageRequest.of(0, 3));
    assertThat(rows).hasSize(3);
  }

  @Test
  void find_noParams_returnsAll() {
    Instant now = Instant.now();
    String uuid1 = UUID.randomUUID().toString();
    String uuid2 = UUID.randomUUID().toString();

    repository.save(
        TableOperationsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(uuid1)
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION.name())
            .completedAt(now)
            .status(HistoryStatus.SUCCESS.name())
            .build());
    repository.save(
        TableOperationsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(uuid2)
            .databaseName("db2")
            .tableName("tbl2")
            .operationType(OperationType.ORPHAN_FILES_DELETION.name())
            .completedAt(now.plusSeconds(1))
            .status(HistoryStatus.FAILED.name())
            .build());

    List<TableOperationsHistoryRow> rows =
        repository.find(null, null, null, null, null, null, null, PageRequest.of(0, 100));
    assertThat(rows).hasSize(2);
    // Newest first
    assertThat(rows.get(0).getStatus()).isEqualTo(HistoryStatus.FAILED.name());
  }

  @Test
  void find_byStatusAndTimeWindow() {
    Instant old = Instant.parse("2024-01-01T00:00:00Z");
    Instant recent = Instant.parse("2024-06-01T00:00:00Z");
    String tableUuid = UUID.randomUUID().toString();

    repository.save(
        TableOperationsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION.name())
            .completedAt(old)
            .status(HistoryStatus.SUCCESS.name())
            .build());
    repository.save(
        TableOperationsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION.name())
            .completedAt(recent)
            .status(HistoryStatus.FAILED.name())
            .build());

    // Filter by status
    List<TableOperationsHistoryRow> failed =
        repository.find(
            null,
            null,
            null,
            null,
            HistoryStatus.FAILED.name(),
            null,
            null,
            PageRequest.of(0, 100));
    assertThat(failed).hasSize(1);
    assertThat(failed.get(0).getCompletedAt()).isEqualTo(recent);

    // Filter by time window
    Instant cutoff = Instant.parse("2024-03-01T00:00:00Z");
    List<TableOperationsHistoryRow> afterCutoff =
        repository.find(null, null, null, null, null, cutoff, null, PageRequest.of(0, 100));
    assertThat(afterCutoff).hasSize(1);
    assertThat(afterCutoff.get(0).getCompletedAt()).isEqualTo(recent);
  }
}
