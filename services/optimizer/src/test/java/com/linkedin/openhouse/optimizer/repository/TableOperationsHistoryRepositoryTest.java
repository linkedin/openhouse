package com.linkedin.openhouse.optimizer.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.api.model.JobResult;
import com.linkedin.openhouse.optimizer.api.model.OperationHistoryStatus;
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
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .submittedAt(t1)
            .status(OperationHistoryStatus.SUCCESS)
            .jobId("job-001")
            .build());

    repository.save(
        TableOperationsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .submittedAt(t2)
            .status(OperationHistoryStatus.FAILED)
            .jobId("job-002")
            .result(JobResult.builder().errorMessage("out of memory").errorType("OOM").build())
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
              .operationType(OperationType.ORPHAN_FILES_DELETION)
              .submittedAt(now.plusSeconds(i))
              .status(OperationHistoryStatus.SUCCESS)
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
              .operationType(OperationType.ORPHAN_FILES_DELETION)
              .submittedAt(now.plusSeconds(i))
              .status(OperationHistoryStatus.SUCCESS)
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
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .submittedAt(now)
            .status(OperationHistoryStatus.SUCCESS)
            .build());
    repository.save(
        TableOperationsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(uuid2)
            .databaseName("db2")
            .tableName("tbl2")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .submittedAt(now.plusSeconds(1))
            .status(OperationHistoryStatus.FAILED)
            .build());

    List<TableOperationsHistoryRow> rows =
        repository.find(null, null, null, null, null, null, null, PageRequest.of(0, 100));
    assertThat(rows).hasSize(2);
    // Newest first
    assertThat(rows.get(0).getStatus()).isEqualTo(OperationHistoryStatus.FAILED);
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
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .submittedAt(old)
            .status(OperationHistoryStatus.SUCCESS)
            .build());
    repository.save(
        TableOperationsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .submittedAt(recent)
            .status(OperationHistoryStatus.FAILED)
            .build());

    // Filter by status
    List<TableOperationsHistoryRow> failed =
        repository.find(
            null,
            null,
            null,
            null,
            OperationHistoryStatus.FAILED,
            null,
            null,
            PageRequest.of(0, 100));
    assertThat(failed).hasSize(1);
    assertThat(failed.get(0).getSubmittedAt()).isEqualTo(recent);

    // Filter by time window
    Instant cutoff = Instant.parse("2024-03-01T00:00:00Z");
    List<TableOperationsHistoryRow> afterCutoff =
        repository.find(null, null, null, null, null, cutoff, null, PageRequest.of(0, 100));
    assertThat(afterCutoff).hasSize(1);
    assertThat(afterCutoff.get(0).getSubmittedAt()).isEqualTo(recent);
  }
}
