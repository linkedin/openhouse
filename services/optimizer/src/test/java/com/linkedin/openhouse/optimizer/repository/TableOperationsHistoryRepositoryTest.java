package com.linkedin.openhouse.optimizer.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.db.HistoryStatus;
import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.TableOperationsHistoryRow;
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
  void findByTableUuid_returnsRowsNewestFirst() {
    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");
    Instant t2 = Instant.parse("2024-01-02T10:00:00Z");
    String tableUuid = UUID.randomUUID().toString();
    String idOlder = UUID.randomUUID().toString();
    String idNewer = UUID.randomUUID().toString();

    repository.save(
        TableOperationsHistoryRow.builder()
            .id(idOlder)
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .completedAt(t1)
            .status(HistoryStatus.SUCCESS)
            .build());

    repository.save(
        TableOperationsHistoryRow.builder()
            .id(idNewer)
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .completedAt(t2)
            .status(HistoryStatus.FAILED)
            .build());

    List<TableOperationsHistoryRow> rows = repository.find(tableUuid, PageRequest.of(0, 10));

    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getId()).isEqualTo(idNewer);
    assertThat(rows.get(1).getId()).isEqualTo(idOlder);
  }

  @Test
  void findByTableUuid_respectsLimit() {
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
              .completedAt(now.plusSeconds(i))
              .status(HistoryStatus.SUCCESS)
              .build());
    }

    List<TableOperationsHistoryRow> rows = repository.find(tableUuid, PageRequest.of(0, 3));
    assertThat(rows).hasSize(3);
  }

  @Test
  void findLatestPerTable_returnsOneRowPerTableUuid() {
    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");
    Instant t2 = Instant.parse("2024-02-01T10:00:00Z");
    String tableUuid = UUID.randomUUID().toString();
    String otherUuid = UUID.randomUUID().toString();

    repository.save(
        TableOperationsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .completedAt(t1)
            .status(HistoryStatus.SUCCESS)
            .build());
    repository.save(
        TableOperationsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(tableUuid)
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .completedAt(t2)
            .status(HistoryStatus.FAILED)
            .build());
    repository.save(
        TableOperationsHistoryRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(otherUuid)
            .databaseName("db1")
            .tableName("tbl2")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .completedAt(t1)
            .status(HistoryStatus.SUCCESS)
            .build());

    List<TableOperationsHistoryRow> latest =
        repository.findLatest(OperationType.ORPHAN_FILES_DELETION, PageRequest.of(0, 10_000));

    assertThat(latest).hasSize(2);
    TableOperationsHistoryRow forTarget =
        latest.stream().filter(r -> r.getTableUuid().equals(tableUuid)).findFirst().orElseThrow();
    assertThat(forTarget.getCompletedAt()).isEqualTo(t2);
    assertThat(forTarget.getStatus()).isEqualTo(HistoryStatus.FAILED);
  }
}
