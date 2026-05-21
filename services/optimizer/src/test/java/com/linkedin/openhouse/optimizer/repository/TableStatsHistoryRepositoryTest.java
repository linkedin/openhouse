package com.linkedin.openhouse.optimizer.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.db.CommitDeltaMetrics;
import com.linkedin.openhouse.optimizer.db.SnapshotMetrics;
import com.linkedin.openhouse.optimizer.db.TableStatsHistoryRow;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
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
class TableStatsHistoryRepositoryTest {

  @Autowired TableStatsHistoryRepository repository;

  @Test
  void saveAndFind() {
    String tableUuid = UUID.randomUUID().toString();
    Instant now = Instant.now();

    repository.save(buildRow(tableUuid, "db1", "tbl1", 10L, 2L, now.minus(2, ChronoUnit.HOURS)));
    repository.save(buildRow(tableUuid, "db1", "tbl1", 5L, 1L, now.minus(1, ChronoUnit.HOURS)));
    repository.save(buildRow(tableUuid, "db1", "tbl1", 3L, 0L, now));

    List<TableStatsHistoryRow> rows =
        repository.find(tableUuid, Optional.empty(), PageRequest.of(0, 100));

    assertThat(rows).hasSize(3);
    // newest first
    assertThat(rows.get(0).getDelta().getNumFilesAdded()).isEqualTo(3L);
    assertThat(rows.get(2).getDelta().getNumFilesAdded()).isEqualTo(10L);
  }

  @Test
  void find_respectsLimit() {
    String tableUuid = UUID.randomUUID().toString();
    Instant now = Instant.now();

    for (int i = 0; i < 5; i++) {
      repository.save(buildRow(tableUuid, "db1", "tbl1", i, 0L, now.minus(i, ChronoUnit.HOURS)));
    }

    List<TableStatsHistoryRow> rows =
        repository.find(tableUuid, Optional.empty(), PageRequest.of(0, 3));

    assertThat(rows).hasSize(3);
  }

  @Test
  void find_withSince_filtersOlderRows() {
    String tableUuid = UUID.randomUUID().toString();
    Instant now = Instant.now();
    Instant cutoff = now.minus(90, ChronoUnit.MINUTES);

    repository.save(buildRow(tableUuid, "db1", "tbl1", 10L, 2L, now.minus(2, ChronoUnit.HOURS)));
    repository.save(buildRow(tableUuid, "db1", "tbl1", 5L, 1L, now.minus(1, ChronoUnit.HOURS)));
    repository.save(buildRow(tableUuid, "db1", "tbl1", 3L, 0L, now));

    List<TableStatsHistoryRow> rows =
        repository.find(tableUuid, Optional.of(cutoff), PageRequest.of(0, 100));

    // only the 2 rows within the last 90 minutes
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getDelta().getNumFilesAdded()).isEqualTo(3L);
  }

  @Test
  void find_isolatesByTableUuid() {
    String uuid1 = UUID.randomUUID().toString();
    String uuid2 = UUID.randomUUID().toString();
    Instant now = Instant.now();

    repository.save(buildRow(uuid1, "db1", "tbl1", 10L, 0L, now));
    repository.save(buildRow(uuid2, "db2", "tbl2", 20L, 0L, now));

    assertThat(repository.find(uuid1, Optional.empty(), PageRequest.of(0, 100))).hasSize(1);
    assertThat(repository.find(uuid2, Optional.empty(), PageRequest.of(0, 100))).hasSize(1);
  }

  @Test
  void callerSetIdIsPreserved() {
    String tableUuid = UUID.randomUUID().toString();
    String id1 = UUID.randomUUID().toString();
    String id2 = UUID.randomUUID().toString();
    Instant now = Instant.now();

    TableStatsHistoryRow row1 =
        repository.save(buildRow(id1, tableUuid, "db1", "tbl1", 1L, 0L, now));
    TableStatsHistoryRow row2 =
        repository.save(buildRow(id2, tableUuid, "db1", "tbl1", 2L, 0L, now));

    assertThat(row1.getId()).isEqualTo(id1);
    assertThat(row2.getId()).isEqualTo(id2);
    assertThat(repository.findById(id1)).isPresent();
    assertThat(repository.findById(id2)).isPresent();
  }

  private static TableStatsHistoryRow buildRow(
      String tableUuid,
      String databaseName,
      String tableName,
      long numFilesAdded,
      long numFilesDeleted,
      Instant recordedAt) {
    return buildRow(
        UUID.randomUUID().toString(),
        tableUuid,
        databaseName,
        tableName,
        numFilesAdded,
        numFilesDeleted,
        recordedAt);
  }

  private static TableStatsHistoryRow buildRow(
      String id,
      String tableUuid,
      String databaseName,
      String tableName,
      long numFilesAdded,
      long numFilesDeleted,
      Instant recordedAt) {
    return TableStatsHistoryRow.builder()
        .id(id)
        .tableUuid(tableUuid)
        .databaseName(databaseName)
        .tableName(tableName)
        .snapshot(SnapshotMetrics.builder().tableSizeBytes(1024L).build())
        .delta(
            CommitDeltaMetrics.builder()
                .numFilesAdded(numFilesAdded)
                .numFilesDeleted(numFilesDeleted)
                .build())
        .recordedAt(recordedAt)
        .build();
  }
}
