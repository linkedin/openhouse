package com.linkedin.openhouse.optimizer.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.db.OperationStatus;
import com.linkedin.openhouse.optimizer.db.OperationType;
import com.linkedin.openhouse.optimizer.db.TableOperationsRow;
import java.time.Instant;
import java.util.List;
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
class TableOperationsRepositoryTest {

  private static final Pageable PAGE = PageRequest.of(0, 10_000);

  @Autowired TableOperationsRepository repository;

  @Test
  void saveAndFindById() {
    String id = UUID.randomUUID().toString();

    repository.save(pendingRow(id, "tbl1"));

    Optional<TableOperationsRow> found = repository.findById(id);
    assertThat(found).isPresent();
    assertThat(found.get().getStatus()).isEqualTo(OperationStatus.PENDING);
  }

  @Test
  void find_noFilters_returnsAll() {
    repository.save(pendingRow(UUID.randomUUID().toString(), "tbl1"));
    repository.save(scheduledRow(UUID.randomUUID().toString(), "tbl2"));

    List<TableOperationsRow> rows =
        repository.find(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            PAGE);
    assertThat(rows).hasSize(2);
  }

  @Test
  void find_byStatus() {
    repository.save(pendingRow(UUID.randomUUID().toString(), "tbl1"));
    repository.save(scheduledRow(UUID.randomUUID().toString(), "tbl2"));

    List<TableOperationsRow> pending =
        repository.find(
            Optional.empty(),
            Optional.of(OperationStatus.PENDING),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            PAGE);
    assertThat(pending).hasSize(1);
    assertThat(pending.get(0).getStatus()).isEqualTo(OperationStatus.PENDING);

    List<TableOperationsRow> scheduled =
        repository.find(
            Optional.empty(),
            Optional.of(OperationStatus.SCHEDULED),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            PAGE);
    assertThat(scheduled).hasSize(1);
    assertThat(scheduled.get(0).getStatus()).isEqualTo(OperationStatus.SCHEDULED);
  }

  @Test
  void find_byDatabaseAndTable() {
    repository.save(pendingRow(UUID.randomUUID().toString(), "tbl1", "db1"));
    repository.save(pendingRow(UUID.randomUUID().toString(), "tbl2", "db2"));

    assertThat(
            repository.find(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("db1"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                PAGE))
        .hasSize(1);
    assertThat(
            repository.find(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("db2"),
                Optional.of("tbl2"),
                Optional.empty(),
                Optional.empty(),
                PAGE))
        .hasSize(1);
    assertThat(
            repository.find(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of("db1"),
                Optional.of("tbl2"),
                Optional.empty(),
                Optional.empty(),
                PAGE))
        .isEmpty();
  }

  @Test
  void find_byScheduledAtAndIds_resolvesClaimedSubset() {
    String idA = UUID.randomUUID().toString();
    String idB = UUID.randomUUID().toString();
    String idC = UUID.randomUUID().toString();
    repository.save(pendingRow(idA, "tbl_a"));
    repository.save(pendingRow(idB, "tbl_b"));
    // idC is already SCHEDULING with an older watermark — must NOT appear.
    repository.save(
        TableOperationsRow.builder()
            .id(idC)
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl_c")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(OperationStatus.SCHEDULING)
            .createdAt(Instant.now())
            .scheduledAt(Instant.now().minusSeconds(60))
            .build());

    Instant now = Instant.now();
    int transitioned =
        repository.updateBatch(
            List.of(idA, idB, idC),
            OperationStatus.PENDING,
            OperationStatus.SCHEDULING,
            Optional.of(now),
            Optional.empty());
    assertThat(transitioned).isEqualTo(2);

    List<String> claimedIds =
        repository
            .find(
                Optional.empty(),
                Optional.of(OperationStatus.SCHEDULING),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(now),
                Optional.of(List.of(idA, idB, idC)),
                PAGE)
            .stream()
            .map(TableOperationsRow::getId)
            .collect(Collectors.toList());
    assertThat(claimedIds).containsExactlyInAnyOrder(idA, idB);
  }

  @Test
  void updateBatch_schedulingToScheduled_setsJobIdAndPreservesScheduledAt() {
    String id = UUID.randomUUID().toString();
    Instant claimedAt = Instant.parse("2026-05-20T16:42:43Z");
    repository.save(
        TableOperationsRow.builder()
            .id(id)
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(OperationStatus.SCHEDULING)
            .createdAt(Instant.now())
            .scheduledAt(claimedAt)
            .build());

    int updated =
        repository.updateBatch(
            List.of(id),
            OperationStatus.SCHEDULING,
            OperationStatus.SCHEDULED,
            Optional.empty(),
            Optional.of("job-123"));
    assertThat(updated).isEqualTo(1);

    TableOperationsRow row = repository.findById(id).orElseThrow();
    assertThat(row.getStatus()).isEqualTo(OperationStatus.SCHEDULED);
    assertThat(row.getJobId()).isEqualTo("job-123");
    assertThat(row.getScheduledAt()).isEqualTo(claimedAt);
  }

  @Test
  void updateBatch_schedulingToPending_leavesScheduledAtUntouched() {
    // scheduledAt is intentionally NOT cleared on revert. Status is the source of truth; the
    // stale watermark gets overwritten on the next PENDING → SCHEDULING transition.
    String id = UUID.randomUUID().toString();
    Instant claimedAt = Instant.parse("2026-05-20T16:42:43Z");
    repository.save(
        TableOperationsRow.builder()
            .id(id)
            .tableUuid(UUID.randomUUID().toString())
            .databaseName("db1")
            .tableName("tbl1")
            .operationType(OperationType.ORPHAN_FILES_DELETION)
            .status(OperationStatus.SCHEDULING)
            .createdAt(Instant.now())
            .scheduledAt(claimedAt)
            .build());

    int reverted =
        repository.updateBatch(
            List.of(id),
            OperationStatus.SCHEDULING,
            OperationStatus.PENDING,
            Optional.empty(),
            Optional.empty());
    assertThat(reverted).isEqualTo(1);

    TableOperationsRow row = repository.findById(id).orElseThrow();
    assertThat(row.getStatus()).isEqualTo(OperationStatus.PENDING);
    assertThat(row.getScheduledAt()).isEqualTo(claimedAt);
  }

  @Test
  void updateBatch_skipsRowsNotInFromStatus() {
    String pendingId = UUID.randomUUID().toString();
    String scheduledId = UUID.randomUUID().toString();
    repository.save(pendingRow(pendingId, "tbl_a"));
    repository.save(scheduledRow(scheduledId, "tbl_b"));

    int transitioned =
        repository.updateBatch(
            List.of(pendingId, scheduledId),
            OperationStatus.PENDING,
            OperationStatus.SCHEDULING,
            Optional.of(Instant.now()),
            Optional.empty());
    assertThat(transitioned).isEqualTo(1);

    assertThat(repository.findById(pendingId).orElseThrow().getStatus())
        .isEqualTo(OperationStatus.SCHEDULING);
    assertThat(repository.findById(scheduledId).orElseThrow().getStatus())
        .isEqualTo(OperationStatus.SCHEDULED);
  }

  @Test
  void cancel_deletesOnlyPendingRows() {
    String pendingId = UUID.randomUUID().toString();
    String scheduledId = UUID.randomUUID().toString();
    repository.save(pendingRow(pendingId, "tbl_p"));
    repository.save(scheduledRow(scheduledId, "tbl_s"));

    int deleted = repository.cancel(List.of(pendingId, scheduledId));
    assertThat(deleted).isEqualTo(1);

    assertThat(repository.findById(pendingId)).isEmpty();
    assertThat(repository.findById(scheduledId)).isPresent();
  }

  // --- helpers ---

  private TableOperationsRow pendingRow(String id, String tableName) {
    return pendingRow(id, tableName, "db1");
  }

  private TableOperationsRow pendingRow(String id, String tableName, String databaseName) {
    return TableOperationsRow.builder()
        .id(id)
        .tableUuid(UUID.randomUUID().toString())
        .databaseName(databaseName)
        .tableName(tableName)
        .operationType(OperationType.ORPHAN_FILES_DELETION)
        .status(OperationStatus.PENDING)
        .createdAt(Instant.now())
        .build();
  }

  private TableOperationsRow scheduledRow(String id, String tableName) {
    return TableOperationsRow.builder()
        .id(id)
        .tableUuid(UUID.randomUUID().toString())
        .databaseName("db1")
        .tableName(tableName)
        .operationType(OperationType.ORPHAN_FILES_DELETION)
        .status(OperationStatus.SCHEDULED)
        .createdAt(Instant.now())
        .scheduledAt(Instant.now())
        .jobId("job-" + id)
        .build();
  }
}
