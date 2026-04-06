package com.linkedin.openhouse.optimizer.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.api.model.TableStats;
import com.linkedin.openhouse.optimizer.entity.TableStatsRow;
import java.time.Instant;
import java.util.Map;
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
class TableStatsRepositoryTest {

  @Autowired TableStatsRepository repository;

  @Test
  void saveAndFindById() {
    String tableUuid = UUID.randomUUID().toString();
    TableStats stats =
        TableStats.builder()
            .snapshot(
                TableStats.SnapshotMetrics.builder().clusterId("cl1").tableSizeBytes(1024L).build())
            .delta(TableStats.CommitDelta.builder().numFilesAdded(3L).numFilesDeleted(1L).build())
            .build();

    repository.save(
        TableStatsRow.builder()
            .tableUuid(tableUuid)
            .databaseId("db1")
            .tableName("tbl1")
            .stats(stats)
            .tableProperties(Map.of("maintenance.optimizer.ofd.enabled", "true"))
            .updatedAt(Instant.now())
            .build());

    Optional<TableStatsRow> found = repository.findById(tableUuid);
    assertThat(found).isPresent();
    assertThat(found.get().getDatabaseId()).isEqualTo("db1");
    assertThat(found.get().getStats().getSnapshot().getTableSizeBytes()).isEqualTo(1024L);
    assertThat(found.get().getTableProperties())
        .containsEntry("maintenance.optimizer.ofd.enabled", "true");
  }

  @Test
  void upsert_overwritesPreviousStats() {
    String tableUuid = UUID.randomUUID().toString();

    repository.save(
        TableStatsRow.builder()
            .tableUuid(tableUuid)
            .databaseId("db1")
            .tableName("tbl1")
            .stats(
                TableStats.builder()
                    .snapshot(TableStats.SnapshotMetrics.builder().tableSizeBytes(100L).build())
                    .build())
            .updatedAt(Instant.now())
            .build());

    repository.save(
        TableStatsRow.builder()
            .tableUuid(tableUuid)
            .databaseId("db1")
            .tableName("tbl1")
            .stats(
                TableStats.builder()
                    .snapshot(TableStats.SnapshotMetrics.builder().tableSizeBytes(200L).build())
                    .build())
            .updatedAt(Instant.now())
            .build());

    assertThat(repository.findAll()).hasSize(1);
    assertThat(repository.findById(tableUuid).get().getStats().getSnapshot().getTableSizeBytes())
        .isEqualTo(200L);
  }

  @Test
  void findFiltered_noParams_returnsAll() {
    repository.save(
        TableStatsRow.builder()
            .tableUuid(UUID.randomUUID().toString())
            .databaseId("db1")
            .tableName("tbl1")
            .stats(
                TableStats.builder()
                    .snapshot(TableStats.SnapshotMetrics.builder().tableSizeBytes(100L).build())
                    .build())
            .updatedAt(Instant.now())
            .build());
    repository.save(
        TableStatsRow.builder()
            .tableUuid(UUID.randomUUID().toString())
            .databaseId("db2")
            .tableName("tbl2")
            .stats(
                TableStats.builder()
                    .snapshot(TableStats.SnapshotMetrics.builder().tableSizeBytes(200L).build())
                    .build())
            .updatedAt(Instant.now())
            .build());

    assertThat(repository.findFiltered(null, null, null)).hasSize(2);
  }

  @Test
  void findFiltered_byDatabase() {
    repository.save(
        TableStatsRow.builder()
            .tableUuid(UUID.randomUUID().toString())
            .databaseId("db1")
            .tableName("tbl1")
            .stats(
                TableStats.builder()
                    .snapshot(TableStats.SnapshotMetrics.builder().tableSizeBytes(100L).build())
                    .build())
            .updatedAt(Instant.now())
            .build());
    repository.save(
        TableStatsRow.builder()
            .tableUuid(UUID.randomUUID().toString())
            .databaseId("db2")
            .tableName("tbl2")
            .stats(
                TableStats.builder()
                    .snapshot(TableStats.SnapshotMetrics.builder().tableSizeBytes(200L).build())
                    .build())
            .updatedAt(Instant.now())
            .build());

    assertThat(repository.findFiltered("db1", null, null)).hasSize(1);
    assertThat(repository.findFiltered("db1", null, null).get(0).getDatabaseId()).isEqualTo("db1");
  }
}
