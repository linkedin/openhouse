package com.linkedin.openhouse.housetables.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationCheckpoint;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationCheckpointUpdate;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationDestination;
import com.linkedin.openhouse.housetables.repository.ReplicationStateStore;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.web.server.ResponseStatusException;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
class ReplicationDestinationSchemaTest {

  private static final String DATABASE_ID = "replication_destination_schema_test";
  private static final String SOURCE_CLUSTER_ID = "source_cluster";
  private static final String SOURCE_TABLE_UUID = "source-table-uuid";
  private static final long SOURCE_CREATION_TIME = 100L;
  private static final String DESTINATION_CLUSTER_ID = "destination_cluster";
  private static final String DESTINATION_TABLE_UUID = "destination-table-uuid";
  private static final long DESTINATION_CREATION_TIME = 200L;

  @Autowired DataSource dataSource;

  @Autowired ReplicationStateStore replicationStateStore;

  private JdbcTemplate jdbcTemplate() {
    return new JdbcTemplate(dataSource);
  }

  @AfterEach
  void cleanUp() {
    JdbcTemplate jdbcTemplate = jdbcTemplate();
    jdbcTemplate.update(
        "DELETE FROM replication_checkpoint WHERE UPPER(source_cluster_id) = ?",
        SOURCE_CLUSTER_ID.toUpperCase(java.util.Locale.ROOT));
    jdbcTemplate.update(
        "DELETE FROM replication_destination WHERE UPPER(source_cluster_id) = ?",
        SOURCE_CLUSTER_ID.toUpperCase(java.util.Locale.ROOT));
  }

  @Test
  void destinationAssociationUsesStableTableGenerationsAndMutableLocators() {
    JdbcTemplate jdbcTemplate = jdbcTemplate();
    insertDestination(jdbcTemplate, DESTINATION_CLUSTER_ID, DESTINATION_TABLE_UUID);

    assertThatThrownBy(
            () -> insertDestination(jdbcTemplate, DESTINATION_CLUSTER_ID, DESTINATION_TABLE_UUID))
        .isInstanceOf(DataIntegrityViolationException.class);

    jdbcTemplate.update(
        "UPDATE replication_destination SET source_table_id = ?, destination_table_id = ? "
            + "WHERE source_cluster_id = ? AND source_table_uuid = ? "
            + "AND source_creation_time = ? AND destination_cluster_id = ? "
            + "AND destination_table_uuid = ? AND destination_creation_time = ?",
        "renamed_source",
        "renamed_destination",
        SOURCE_CLUSTER_ID,
        SOURCE_TABLE_UUID,
        SOURCE_CREATION_TIME,
        DESTINATION_CLUSTER_ID,
        DESTINATION_TABLE_UUID,
        DESTINATION_CREATION_TIME);

    assertThat(
            jdbcTemplate.queryForObject(
                "SELECT destination_table_id FROM replication_destination "
                    + "WHERE source_cluster_id = ? AND source_table_uuid = ? "
                    + "AND source_creation_time = ? AND destination_cluster_id = ? "
                    + "AND destination_table_uuid = ? AND destination_creation_time = ?",
                String.class,
                SOURCE_CLUSTER_ID,
                SOURCE_TABLE_UUID,
                SOURCE_CREATION_TIME,
                DESTINATION_CLUSTER_ID,
                DESTINATION_TABLE_UUID,
                DESTINATION_CREATION_TIME))
        .isEqualTo("renamed_destination");
  }

  @Test
  void checkpointProgressIsStoredSeparatelyForEachSourceDestinationEdge() {
    JdbcTemplate jdbcTemplate = jdbcTemplate();
    insertDestination(jdbcTemplate, DESTINATION_CLUSTER_ID, DESTINATION_TABLE_UUID);
    jdbcTemplate.update(
        "INSERT INTO replication_checkpoint (source_cluster_id, source_table_uuid, "
            + "source_creation_time, destination_cluster_id, destination_table_uuid, "
            + "destination_creation_time, source_snapshot_id, destination_snapshot_id, "
            + "destination_table_version, revision) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        SOURCE_CLUSTER_ID,
        SOURCE_TABLE_UUID,
        SOURCE_CREATION_TIME,
        DESTINATION_CLUSTER_ID,
        DESTINATION_TABLE_UUID,
        DESTINATION_CREATION_TIME,
        101L,
        201L,
        "metadata-v2",
        1L);

    assertThat(
            jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM replication_destination WHERE source_cluster_id = ?",
                Integer.class,
                SOURCE_CLUSTER_ID))
        .isEqualTo(1);
    assertThat(
            jdbcTemplate.queryForObject(
                "SELECT source_snapshot_id FROM replication_checkpoint "
                    + "WHERE source_cluster_id = ? AND source_table_uuid = ? "
                    + "AND source_creation_time = ? AND destination_cluster_id = ? "
                    + "AND destination_table_uuid = ? AND destination_creation_time = ?",
                Long.class,
                SOURCE_CLUSTER_ID,
                SOURCE_TABLE_UUID,
                SOURCE_CREATION_TIME,
                DESTINATION_CLUSTER_ID,
                DESTINATION_TABLE_UUID,
                DESTINATION_CREATION_TIME))
        .isEqualTo(101L);
  }

  @Test
  void destinationLocatorsUseCompareAndSetAndRetriesAreIdempotent() {
    ReplicationDestination created =
        replicationStateStore.putDestination(destination(0L, "source_table", "destination_table"));
    assertThat(created.getVersion()).isEqualTo(1L);

    ReplicationDestination retry =
        replicationStateStore.putDestination(destination(0L, "source_table", "destination_table"));
    assertThat(retry.getVersion()).isEqualTo(1L);

    ReplicationDestination renamed =
        replicationStateStore.putDestination(
            destination(1L, "renamed_source", "renamed_destination"));
    assertThat(renamed.getVersion()).isEqualTo(2L);

    assertThatThrownBy(
            () ->
                replicationStateStore.putDestination(
                    destination(1L, "stale_source", "stale_destination")))
        .isInstanceOf(ResponseStatusException.class)
        .extracting(error -> ((ResponseStatusException) error).getStatus())
        .isEqualTo(HttpStatus.CONFLICT);
    assertThat(
            replicationStateStore
                .findDestinations(SOURCE_CLUSTER_ID, SOURCE_TABLE_UUID, SOURCE_CREATION_TIME)
                .get(0)
                .getDestination()
                .getDestinationTableId())
        .isEqualTo("renamed_destination");
  }

  @Test
  void checkpointUsesPerEdgeCasAndDoesNotAdvanceOnAStaleCommitVersion() {
    replicationStateStore.putDestination(destination(0L, "source_table", "destination_table"));
    ReplicationCheckpointUpdate first = checkpointUpdate(0L, "destination-version-1", 101L);
    ReplicationCheckpoint advanced = replicationStateStore.advanceCheckpoint(first);
    assertThat(advanced.getRevision()).isEqualTo(1L);

    assertThat(replicationStateStore.advanceCheckpoint(first)).isEqualTo(advanced);

    ReplicationCheckpointUpdate stale = checkpointUpdate(0L, "destination-version-2", 102L);
    assertThatThrownBy(() -> replicationStateStore.advanceCheckpoint(stale))
        .isInstanceOf(ResponseStatusException.class)
        .extracting(error -> ((ResponseStatusException) error).getStatus())
        .isEqualTo(HttpStatus.CONFLICT);
    assertThat(
            replicationStateStore.findCheckpoint(
                SOURCE_CLUSTER_ID,
                SOURCE_TABLE_UUID,
                SOURCE_CREATION_TIME,
                DESTINATION_CLUSTER_ID,
                DESTINATION_TABLE_UUID,
                DESTINATION_CREATION_TIME))
        .isEqualTo(advanced);
  }

  private ReplicationDestination destination(
      long expectedVersion, String sourceTableId, String destinationTableId) {
    return ReplicationDestination.builder()
        .sourceClusterId(SOURCE_CLUSTER_ID)
        .sourceTableUUID(SOURCE_TABLE_UUID)
        .sourceCreationTime(SOURCE_CREATION_TIME)
        .sourceDatabaseId("source_db")
        .sourceTableId(sourceTableId)
        .destinationClusterId(DESTINATION_CLUSTER_ID)
        .destinationTableUUID(DESTINATION_TABLE_UUID)
        .destinationCreationTime(DESTINATION_CREATION_TIME)
        .destinationDatabaseId(DATABASE_ID)
        .destinationTableId(destinationTableId)
        .expectedVersion(expectedVersion)
        .build();
  }

  private ReplicationCheckpointUpdate checkpointUpdate(
      long expectedRevision, String destinationTableVersion, long sourceSnapshotId) {
    return ReplicationCheckpointUpdate.builder()
        .sourceClusterId(SOURCE_CLUSTER_ID)
        .sourceTableUUID(SOURCE_TABLE_UUID)
        .sourceCreationTime(SOURCE_CREATION_TIME)
        .destinationClusterId(DESTINATION_CLUSTER_ID)
        .destinationTableUUID(DESTINATION_TABLE_UUID)
        .destinationCreationTime(DESTINATION_CREATION_TIME)
        .sourceDatabaseId("source_db")
        .sourceTableId("source_table")
        .destinationDatabaseId(DATABASE_ID)
        .destinationTableId("destination_table")
        .expectedRevision(expectedRevision)
        .sourceTableVersion("source-version-1")
        .sourceSnapshotId(sourceSnapshotId)
        .destinationSnapshotId(201L)
        .destinationTableVersion(destinationTableVersion)
        .build();
  }

  private void insertDestination(
      JdbcTemplate jdbcTemplate, String destinationClusterId, String destinationTableUuid) {
    jdbcTemplate.update(
        "INSERT INTO replication_destination (source_cluster_id, source_table_uuid, "
            + "source_creation_time, source_database_id, source_table_id, destination_cluster_id, "
            + "destination_table_uuid, destination_creation_time, destination_database_id, "
            + "destination_table_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        SOURCE_CLUSTER_ID,
        SOURCE_TABLE_UUID,
        SOURCE_CREATION_TIME,
        "source_db",
        "source_table",
        destinationClusterId,
        destinationTableUuid,
        DESTINATION_CREATION_TIME,
        DATABASE_ID,
        "destination_table");
  }
}
