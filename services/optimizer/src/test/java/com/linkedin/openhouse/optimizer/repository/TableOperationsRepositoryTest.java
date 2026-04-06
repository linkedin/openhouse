package com.linkedin.openhouse.optimizer.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.api.model.OperationStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
import com.linkedin.openhouse.optimizer.entity.TableOperationsRow;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class TableOperationsRepositoryTest {

  @Autowired TableOperationsRepository repository;

  @Test
  void saveAndFindById() {
    String id = UUID.randomUUID().toString();

    TableOperationsRow row =
        TableOperationsRow.builder()
            .id(id)
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(OperationStatus.PENDING)
            .createdAt(Instant.now())
            .build();

    repository.save(row);

    Optional<TableOperationsRow> found = repository.findById(id);
    assertThat(found).isPresent();
    assertThat(found.get().getStatus()).isEqualTo(OperationStatus.PENDING);
  }

  @Test
  void findFiltered_noParams_returnsAll() {
    repository.save(
        TableOperationsRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(OperationStatus.PENDING)
            .createdAt(Instant.now())
            .build());
    repository.save(
        TableOperationsRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl2")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(OperationStatus.SCHEDULED)
            .createdAt(Instant.now())
            .build());

    List<TableOperationsRow> rows = repository.findFiltered(null, null, null, null, null);
    assertThat(rows).hasSize(2);
  }

  @Test
  void findFiltered_byStatus() {
    repository.save(
        TableOperationsRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(OperationStatus.PENDING)
            .createdAt(Instant.now())
            .build());
    repository.save(
        TableOperationsRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl2")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(OperationStatus.SCHEDULED)
            .createdAt(Instant.now())
            .build());

    List<TableOperationsRow> pending =
        repository.findFiltered(null, OperationStatus.PENDING, null, null, null);
    assertThat(pending).hasSize(1);
    assertThat(pending.get(0).getStatus()).isEqualTo(OperationStatus.PENDING);

    List<TableOperationsRow> scheduled =
        repository.findFiltered(null, OperationStatus.SCHEDULED, null, null, null);
    assertThat(scheduled).hasSize(1);
    assertThat(scheduled.get(0).getStatus()).isEqualTo(OperationStatus.SCHEDULED);
  }

  @Test
  void findFiltered_byDatabaseAndTable() {
    repository.save(
        TableOperationsRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(OperationStatus.PENDING)
            .createdAt(Instant.now())
            .build());
    repository.save(
        TableOperationsRow.builder()
            .id(UUID.randomUUID().toString())
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db2")
            .tableName("tbl2")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(OperationStatus.PENDING)
            .createdAt(Instant.now())
            .build());

    assertThat(repository.findFiltered(null, null, "db1", null, null)).hasSize(1);
    assertThat(repository.findFiltered(null, null, "db2", "tbl2", null)).hasSize(1);
    assertThat(repository.findFiltered(null, null, "db1", "tbl2", null)).isEmpty();
  }
}
