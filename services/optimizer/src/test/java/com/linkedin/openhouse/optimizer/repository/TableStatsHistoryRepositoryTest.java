package com.linkedin.openhouse.optimizer.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.api.model.TableStats;
import com.linkedin.openhouse.optimizer.entity.TableStatsHistoryRow;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
class TableStatsHistoryRepositoryTest {

  @Autowired TableStatsHistoryRepository repository;

  @Test
  void saveAndFindByTableUuid() {
    String tableUuid = UUID.randomUUID().toString();
    Instant now = Instant.now();

    repository.save(buildRow(tableUuid, "db1", "tbl1", 10L, 2L, now.minus(2, ChronoUnit.HOURS)));
    repository.save(buildRow(tableUuid, "db1", "tbl1", 5L, 1L, now.minus(1, ChronoUnit.HOURS)));
    repository.save(buildRow(tableUuid, "db1", "tbl1", 3L, 0L, now));

    List<TableStatsHistoryRow> rows = repository.findByTableUuid(tableUuid, PageRequest.of(0, 100));

    assertThat(rows).hasSize(3);
    // newest first
    assertThat(rows.get(0).getStats().getDelta().getNumFilesAdded()).isEqualTo(3L);
    assertThat(rows.get(2).getStats().getDelta().getNumFilesAdded()).isEqualTo(10L);
  }

  @Test
  void findByTableUuid_respectsLimit() {
    String tableUuid = UUID.randomUUID().toString();
    Instant now = Instant.now();

    for (int i = 0; i < 5; i++) {
      repository.save(buildRow(tableUuid, "db1", "tbl1", i, 0L, now.minus(i, ChronoUnit.HOURS)));
    }

    List<TableStatsHistoryRow> rows = repository.findByTableUuid(tableUuid, PageRequest.of(0, 3));

    assertThat(rows).hasSize(3);
  }

  @Test
  void findByTableUuidSince_filtersOlderRows() {
    String tableUuid = UUID.randomUUID().toString();
    Instant now = Instant.now();
    Instant cutoff = now.minus(90, ChronoUnit.MINUTES);

    repository.save(buildRow(tableUuid, "db1", "tbl1", 10L, 2L, now.minus(2, ChronoUnit.HOURS)));
    repository.save(buildRow(tableUuid, "db1", "tbl1", 5L, 1L, now.minus(1, ChronoUnit.HOURS)));
    repository.save(buildRow(tableUuid, "db1", "tbl1", 3L, 0L, now));

    List<TableStatsHistoryRow> rows =
        repository.findByTableUuidSince(tableUuid, cutoff, PageRequest.of(0, 100));

    // only the 2 rows within the last 90 minutes
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getStats().getDelta().getNumFilesAdded()).isEqualTo(3L);
  }

  @Test
  void findByTableUuid_isolatesByTableUuid() {
    String uuid1 = UUID.randomUUID().toString();
    String uuid2 = UUID.randomUUID().toString();
    Instant now = Instant.now();

    repository.save(buildRow(uuid1, "db1", "tbl1", 10L, 0L, now));
    repository.save(buildRow(uuid2, "db2", "tbl2", 20L, 0L, now));

    assertThat(repository.findByTableUuid(uuid1, PageRequest.of(0, 100))).hasSize(1);
    assertThat(repository.findByTableUuid(uuid2, PageRequest.of(0, 100))).hasSize(1);
  }

  @Test
  void autoIncrementId() {
    String tableUuid = UUID.randomUUID().toString();
    Instant now = Instant.now();

    TableStatsHistoryRow row1 = repository.save(buildRow(tableUuid, "db1", "tbl1", 1L, 0L, now));
    TableStatsHistoryRow row2 = repository.save(buildRow(tableUuid, "db1", "tbl1", 2L, 0L, now));

    assertThat(row1.getId()).isNotNull();
    assertThat(row2.getId()).isNotNull();
    assertThat(row2.getId()).isGreaterThan(row1.getId());
  }

  private static TableStatsHistoryRow buildRow(
      String tableUuid,
      String databaseId,
      String tableName,
      long numFilesAdded,
      long numFilesDeleted,
      Instant recordedAt) {
    return TableStatsHistoryRow.builder()
        .tableUuid(tableUuid)
        .databaseId(databaseId)
        .tableName(tableName)
        .stats(
            TableStats.builder()
                .snapshot(
                    TableStats.SnapshotMetrics.builder()
                        .clusterId("cl1")
                        .tableSizeBytes(1024L)
                        .build())
                .delta(
                    TableStats.CommitDelta.builder()
                        .numFilesAdded(numFilesAdded)
                        .numFilesDeleted(numFilesDeleted)
                        .build())
                .build())
        .recordedAt(recordedAt)
        .build();
  }
}
