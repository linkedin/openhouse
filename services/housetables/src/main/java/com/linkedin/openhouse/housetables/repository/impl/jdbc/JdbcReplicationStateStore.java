package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import com.linkedin.openhouse.housetables.api.spec.model.ReplicationCheckpoint;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationCheckpointUpdate;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationDestination;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationEdgeState;
import com.linkedin.openhouse.housetables.repository.ReplicationStateStore;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

/** JDBC persistence for per-edge replication identity and progress. */
@Component
@ConditionalOnExpression("'${cluster.housetables.database.type:IN_MEMORY}' != 'ICEBERG'")
public class JdbcReplicationStateStore implements ReplicationStateStore {
  private final JdbcTemplate jdbcTemplate;

  public JdbcReplicationStateStore(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  @Transactional(readOnly = true)
  public List<ReplicationEdgeState> findDestinations(
      String sourceClusterId, String sourceTableUUID, long sourceCreationTime) {
    return jdbcTemplate.query(
        "SELECT d.*, c.source_table_version, c.source_snapshot_id, "
            + "c.destination_snapshot_id, c.destination_table_version, c.revision "
            + "FROM replication_destination d LEFT JOIN replication_checkpoint c "
            + "ON c.source_cluster_id = d.source_cluster_id "
            + "AND c.source_table_uuid = d.source_table_uuid "
            + "AND c.source_creation_time = d.source_creation_time "
            + "AND c.destination_cluster_id = d.destination_cluster_id "
            + "AND c.destination_table_uuid = d.destination_table_uuid "
            + "AND c.destination_creation_time = d.destination_creation_time "
            + "WHERE d.source_cluster_id = ? AND d.source_table_uuid = ? "
            + "AND d.source_creation_time = ?",
        (resultSet, rowNumber) -> {
          ReplicationDestination destination = mapDestination(resultSet);
          ReplicationCheckpoint checkpoint =
              resultSet.getObject("revision") == null ? null : mapCheckpoint(resultSet);
          return ReplicationEdgeState.builder()
              .destination(destination)
              .checkpoint(checkpoint)
              .build();
        },
        normalizeCluster(sourceClusterId),
        normalizeUuid(sourceTableUUID),
        sourceCreationTime);
  }

  @Override
  @Transactional
  public ReplicationDestination putDestination(ReplicationDestination destination) {
    ReplicationDestinationKey key = destinationKey(destination);
    Optional<ReplicationDestination> current = findDestination(key);
    if (current.isPresent() && sameLocators(current.get(), destination)) {
      return current.get();
    }

    long expectedVersion = destination.getExpectedVersion();
    if (current.isPresent()) {
      ReplicationDestination stored = current.get();
      if (expectedVersion != stored.getVersion()) {
        throw conflict("Destination locator revision is stale");
      }
      int updated =
          jdbcTemplate.update(
              "UPDATE replication_destination SET source_database_id = ?, source_table_id = ?, "
                  + "destination_database_id = ?, destination_table_id = ?, version = version + 1 "
                  + "WHERE source_cluster_id = ? AND source_table_uuid = ? "
                  + "AND source_creation_time = ? AND destination_cluster_id = ? "
                  + "AND destination_table_uuid = ? AND destination_creation_time = ? "
                  + "AND version = ?",
              destination.getSourceDatabaseId(),
              destination.getSourceTableId(),
              destination.getDestinationDatabaseId(),
              destination.getDestinationTableId(),
              key.sourceClusterId(),
              key.sourceTableUUID(),
              key.sourceCreationTime(),
              key.destinationClusterId(),
              key.destinationTableUUID(),
              key.destinationCreationTime(),
              expectedVersion);
      if (updated != 1) {
        throw conflict("Destination locator changed concurrently");
      }
    } else {
      if (expectedVersion != 0L) {
        throw conflict("Destination locator does not exist at the expected revision");
      }
      try {
        jdbcTemplate.update(
            "INSERT INTO replication_destination (source_cluster_id, source_table_uuid, "
                + "source_creation_time, source_database_id, source_table_id, "
                + "destination_cluster_id, destination_table_uuid, destination_creation_time, "
                + "destination_database_id, destination_table_id, version) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
            key.sourceClusterId(),
            key.sourceTableUUID(),
            key.sourceCreationTime(),
            destination.getSourceDatabaseId(),
            destination.getSourceTableId(),
            key.destinationClusterId(),
            key.destinationTableUUID(),
            key.destinationCreationTime(),
            destination.getDestinationDatabaseId(),
            destination.getDestinationTableId());
      } catch (DuplicateKeyException e) {
        throw conflict("Destination association was created concurrently");
      }
    }
    return findDestination(key).orElseThrow(() -> conflict("Destination association disappeared"));
  }

  @Override
  @Transactional(readOnly = true)
  public ReplicationCheckpoint findCheckpoint(
      String sourceClusterId,
      String sourceTableUUID,
      long sourceCreationTime,
      String destinationClusterId,
      String destinationTableUUID,
      long destinationCreationTime) {
    List<ReplicationCheckpoint> checkpoints =
        jdbcTemplate.query(
            "SELECT * FROM replication_checkpoint WHERE source_cluster_id = ? "
                + "AND source_table_uuid = ? AND source_creation_time = ? "
                + "AND destination_cluster_id = ? AND destination_table_uuid = ? "
                + "AND destination_creation_time = ?",
            CHECKPOINT_ROW_MAPPER,
            normalizeCluster(sourceClusterId),
            normalizeUuid(sourceTableUUID),
            sourceCreationTime,
            normalizeCluster(destinationClusterId),
            normalizeUuid(destinationTableUUID),
            destinationCreationTime);
    if (checkpoints.isEmpty()) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Replication checkpoint not found");
    }
    return checkpoints.get(0);
  }

  @Override
  @Transactional
  public ReplicationCheckpoint advanceCheckpoint(ReplicationCheckpointUpdate update) {
    ReplicationDestinationKey key = destinationKey(update);
    Optional<ReplicationDestination> destination = findDestination(key);
    if (!destination.isPresent()) {
      throw conflict("Register the replication destination before advancing its checkpoint");
    }
    if (!sameLocators(destination.get(), update)) {
      throw conflict("Replication locators changed before checkpoint advancement");
    }
    Optional<ReplicationCheckpoint> current = findCheckpointOptional(key);
    if (current.isPresent() && sameCheckpoint(current.get(), update)) {
      return current.get();
    }

    long expectedRevision = update.getExpectedRevision();
    if (current.isPresent()) {
      ReplicationCheckpoint stored = current.get();
      if (expectedRevision != stored.getRevision()) {
        throw conflict("Replication checkpoint revision is stale");
      }
      int updated =
          jdbcTemplate.update(
              "UPDATE replication_checkpoint SET source_table_version = ?, "
                  + "source_snapshot_id = ?, destination_snapshot_id = ?, "
                  + "destination_table_version = ?, revision = revision + 1 "
                  + "WHERE source_cluster_id = ? AND source_table_uuid = ? "
                  + "AND source_creation_time = ? AND destination_cluster_id = ? "
                  + "AND destination_table_uuid = ? AND destination_creation_time = ? "
                  + "AND revision = ?",
              update.getSourceTableVersion(),
              update.getSourceSnapshotId(),
              update.getDestinationSnapshotId(),
              update.getDestinationTableVersion(),
              key.sourceClusterId(),
              key.sourceTableUUID(),
              key.sourceCreationTime(),
              key.destinationClusterId(),
              key.destinationTableUUID(),
              key.destinationCreationTime(),
              expectedRevision);
      if (updated != 1) {
        throw conflict("Replication checkpoint changed concurrently");
      }
    } else {
      if (expectedRevision != 0L) {
        throw conflict("Replication checkpoint does not exist at the expected revision");
      }
      try {
        jdbcTemplate.update(
            "INSERT INTO replication_checkpoint (source_cluster_id, source_table_uuid, "
                + "source_creation_time, destination_cluster_id, destination_table_uuid, "
                + "destination_creation_time, source_table_version, source_snapshot_id, "
                + "destination_snapshot_id, destination_table_version, revision) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
            key.sourceClusterId(),
            key.sourceTableUUID(),
            key.sourceCreationTime(),
            key.destinationClusterId(),
            key.destinationTableUUID(),
            key.destinationCreationTime(),
            update.getSourceTableVersion(),
            update.getSourceSnapshotId(),
            update.getDestinationSnapshotId(),
            update.getDestinationTableVersion());
      } catch (DuplicateKeyException e) {
        throw conflict("Replication checkpoint was created concurrently");
      }
    }
    return findCheckpoint(
        key.sourceClusterId(),
        key.sourceTableUUID(),
        key.sourceCreationTime(),
        key.destinationClusterId(),
        key.destinationTableUUID(),
        key.destinationCreationTime());
  }

  private Optional<ReplicationDestination> findDestination(ReplicationDestinationKey key) {
    List<ReplicationDestination> destinations =
        jdbcTemplate.query(
            "SELECT * FROM replication_destination WHERE source_cluster_id = ? "
                + "AND source_table_uuid = ? AND source_creation_time = ? "
                + "AND destination_cluster_id = ? AND destination_table_uuid = ? "
                + "AND destination_creation_time = ?",
            DESTINATION_ROW_MAPPER,
            key.sourceClusterId(),
            key.sourceTableUUID(),
            key.sourceCreationTime(),
            key.destinationClusterId(),
            key.destinationTableUUID(),
            key.destinationCreationTime());
    return destinations.stream().findFirst();
  }

  private Optional<ReplicationCheckpoint> findCheckpointOptional(ReplicationDestinationKey key) {
    List<ReplicationCheckpoint> checkpoints =
        jdbcTemplate.query(
            "SELECT * FROM replication_checkpoint WHERE source_cluster_id = ? "
                + "AND source_table_uuid = ? AND source_creation_time = ? "
                + "AND destination_cluster_id = ? AND destination_table_uuid = ? "
                + "AND destination_creation_time = ?",
            CHECKPOINT_ROW_MAPPER,
            key.sourceClusterId(),
            key.sourceTableUUID(),
            key.sourceCreationTime(),
            key.destinationClusterId(),
            key.destinationTableUUID(),
            key.destinationCreationTime());
    return checkpoints.stream().findFirst();
  }

  private static ReplicationDestination mapDestination(ResultSet rs) throws SQLException {
    return ReplicationDestination.builder()
        .sourceClusterId(rs.getString("source_cluster_id"))
        .sourceTableUUID(rs.getString("source_table_uuid"))
        .sourceCreationTime(rs.getLong("source_creation_time"))
        .sourceDatabaseId(rs.getString("source_database_id"))
        .sourceTableId(rs.getString("source_table_id"))
        .destinationClusterId(rs.getString("destination_cluster_id"))
        .destinationTableUUID(rs.getString("destination_table_uuid"))
        .destinationCreationTime(rs.getLong("destination_creation_time"))
        .destinationDatabaseId(rs.getString("destination_database_id"))
        .destinationTableId(rs.getString("destination_table_id"))
        .version(rs.getLong("version"))
        .expectedVersion(rs.getLong("version"))
        .build();
  }

  private static ReplicationCheckpoint mapCheckpoint(ResultSet rs) throws SQLException {
    return ReplicationCheckpoint.builder()
        .sourceClusterId(rs.getString("source_cluster_id"))
        .sourceTableUUID(rs.getString("source_table_uuid"))
        .sourceCreationTime(rs.getLong("source_creation_time"))
        .destinationClusterId(rs.getString("destination_cluster_id"))
        .destinationTableUUID(rs.getString("destination_table_uuid"))
        .destinationCreationTime(rs.getLong("destination_creation_time"))
        .sourceTableVersion(rs.getString("source_table_version"))
        .sourceSnapshotId(nullableLong(rs, "source_snapshot_id"))
        .destinationSnapshotId(nullableLong(rs, "destination_snapshot_id"))
        .destinationTableVersion(rs.getString("destination_table_version"))
        .revision(rs.getLong("revision"))
        .build();
  }

  private static Long nullableLong(ResultSet rs, String column) throws SQLException {
    long value = rs.getLong(column);
    return rs.wasNull() ? null : value;
  }

  private static boolean sameLocators(
      ReplicationDestination current, ReplicationDestination requested) {
    return current.getSourceDatabaseId().equals(requested.getSourceDatabaseId())
        && current.getSourceTableId().equals(requested.getSourceTableId())
        && current.getDestinationDatabaseId().equals(requested.getDestinationDatabaseId())
        && current.getDestinationTableId().equals(requested.getDestinationTableId());
  }

  private static boolean sameCheckpoint(
      ReplicationCheckpoint current, ReplicationCheckpointUpdate requested) {
    return current.getSourceTableVersion().equals(requested.getSourceTableVersion())
        && current.getSourceSnapshotId().equals(requested.getSourceSnapshotId())
        && current.getDestinationSnapshotId().equals(requested.getDestinationSnapshotId())
        && current.getDestinationTableVersion().equals(requested.getDestinationTableVersion());
  }

  private static boolean sameLocators(
      ReplicationDestination current, ReplicationCheckpointUpdate requested) {
    return current.getSourceDatabaseId().equalsIgnoreCase(requested.getSourceDatabaseId())
        && current.getSourceTableId().equalsIgnoreCase(requested.getSourceTableId())
        && current.getDestinationDatabaseId().equalsIgnoreCase(requested.getDestinationDatabaseId())
        && current.getDestinationTableId().equalsIgnoreCase(requested.getDestinationTableId());
  }

  private static ReplicationDestinationKey destinationKey(ReplicationDestination destination) {
    return new ReplicationDestinationKey(
        normalizeCluster(destination.getSourceClusterId()),
        normalizeUuid(destination.getSourceTableUUID()),
        destination.getSourceCreationTime(),
        normalizeCluster(destination.getDestinationClusterId()),
        normalizeUuid(destination.getDestinationTableUUID()),
        destination.getDestinationCreationTime());
  }

  private static ReplicationDestinationKey destinationKey(ReplicationCheckpointUpdate update) {
    return new ReplicationDestinationKey(
        normalizeCluster(update.getSourceClusterId()),
        normalizeUuid(update.getSourceTableUUID()),
        update.getSourceCreationTime(),
        normalizeCluster(update.getDestinationClusterId()),
        normalizeUuid(update.getDestinationTableUUID()),
        update.getDestinationCreationTime());
  }

  private static String normalizeCluster(String value) {
    return value.toUpperCase(Locale.ROOT);
  }

  private static String normalizeUuid(String value) {
    return value.toLowerCase(Locale.ROOT);
  }

  private static ResponseStatusException conflict(String message) {
    return new ResponseStatusException(HttpStatus.CONFLICT, message);
  }

  private static final RowMapper<ReplicationDestination> DESTINATION_ROW_MAPPER =
      (rs, rowNumber) -> mapDestination(rs);

  private static final RowMapper<ReplicationCheckpoint> CHECKPOINT_ROW_MAPPER =
      (rs, rowNumber) -> mapCheckpoint(rs);

  private static final class ReplicationDestinationKey {
    private final String sourceClusterId;
    private final String sourceTableUUID;
    private final long sourceCreationTime;
    private final String destinationClusterId;
    private final String destinationTableUUID;
    private final long destinationCreationTime;

    private ReplicationDestinationKey(
        String sourceClusterId,
        String sourceTableUUID,
        long sourceCreationTime,
        String destinationClusterId,
        String destinationTableUUID,
        long destinationCreationTime) {
      this.sourceClusterId = sourceClusterId;
      this.sourceTableUUID = sourceTableUUID;
      this.sourceCreationTime = sourceCreationTime;
      this.destinationClusterId = destinationClusterId;
      this.destinationTableUUID = destinationTableUUID;
      this.destinationCreationTime = destinationCreationTime;
    }

    private String sourceClusterId() {
      return sourceClusterId;
    }

    private String sourceTableUUID() {
      return sourceTableUUID;
    }

    private long sourceCreationTime() {
      return sourceCreationTime;
    }

    private String destinationClusterId() {
      return destinationClusterId;
    }

    private String destinationTableUUID() {
      return destinationTableUUID;
    }

    private long destinationCreationTime() {
      return destinationCreationTime;
    }
  }
}
