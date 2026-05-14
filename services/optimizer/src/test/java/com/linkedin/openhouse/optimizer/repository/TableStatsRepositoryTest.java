package com.linkedin.openhouse.optimizer.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.optimizer.db.SnapshotMetrics;
import com.linkedin.openhouse.optimizer.db.TableStatsRow;
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
    SnapshotMetrics snapshot =
        SnapshotMetrics.builder().clusterId("cl1").tableSizeBytes(1024L).build();

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

    assertThat(repository.find(null, null, null)).hasSize(2);
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

    assertThat(repository.find("db1", null, null)).hasSize(1);
    assertThat(repository.find("db1", null, null).get(0).getDatabaseName()).isEqualTo("db1");
  }
}
