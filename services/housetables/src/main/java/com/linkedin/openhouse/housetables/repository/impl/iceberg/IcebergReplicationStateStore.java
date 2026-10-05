package com.linkedin.openhouse.housetables.repository.impl.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationCheckpoint;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationCheckpointUpdate;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationDestination;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationEdgeState;
import com.linkedin.openhouse.housetables.repository.ReplicationStateStore;
import com.linkedin.openhouse.hts.catalog.model.replication.ReplicationStateIcebergRow;
import com.linkedin.openhouse.hts.catalog.model.replication.ReplicationStateIcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.repository.IcebergHtsRepository;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/** Iceberg persistence for per-edge replication identity and checkpoint state. */
@Component
@ConditionalOnProperty(value = "cluster.housetables.database.type", havingValue = "ICEBERG")
public class IcebergReplicationStateStore implements ReplicationStateStore {
  private final IcebergHtsRepository<
          ReplicationStateIcebergRow, ReplicationStateIcebergRowPrimaryKey>
      destinationRepository;
  private final IcebergHtsRepository<
          ReplicationStateIcebergRow, ReplicationStateIcebergRowPrimaryKey>
      checkpointRepository;
  private final ObjectMapper objectMapper;

  public IcebergReplicationStateStore(
      @Qualifier("replicationDestinationIcebergRepository")
          IcebergHtsRepository<ReplicationStateIcebergRow, ReplicationStateIcebergRowPrimaryKey>
              destinationRepository,
      @Qualifier("replicationCheckpointIcebergRepository")
          IcebergHtsRepository<ReplicationStateIcebergRow, ReplicationStateIcebergRowPrimaryKey>
              checkpointRepository,
      ObjectMapper objectMapper) {
    this.destinationRepository = destinationRepository;
    this.checkpointRepository = checkpointRepository;
    this.objectMapper = objectMapper;
  }

  @Override
  public List<ReplicationEdgeState> findDestinations(
      String sourceClusterId, String sourceTableUUID, long sourceCreationTime) {
    Iterable<ReplicationStateIcebergRow> rows =
        destinationRepository.searchByPartialId(
            ReplicationStateIcebergRowPrimaryKey.builder()
                .sourceClusterId(normalizeCluster(sourceClusterId))
                .sourceTableUUID(normalizeUuid(sourceTableUUID))
                .sourceCreationTime(sourceCreationTime)
                .build());
    List<ReplicationEdgeState> edges = new ArrayList<>();
    for (ReplicationStateIcebergRow row : rows) {
      ReplicationDestination destination = readDestination(row.getPayload());
      ReplicationCheckpoint checkpoint =
          findCheckpointOptional(keyFromRow(row))
              .map(checkpointRow -> readCheckpoint(checkpointRow.getPayload()))
              .orElse(null);
      edges.add(
          ReplicationEdgeState.builder()
              .destination(withCurrentExpectedVersion(destination))
              .checkpoint(checkpoint)
              .build());
    }
    return edges;
  }

  @Override
  public ReplicationDestination putDestination(ReplicationDestination destination) {
    ReplicationStateIcebergRowPrimaryKey key = destinationKey(destination);
    Optional<ReplicationStateIcebergRow> currentRow = find(destinationRepository, key);
    if (currentRow.isPresent()) {
      ReplicationDestination current = readDestination(currentRow.get().getPayload());
      if (sameLocators(current, destination)) {
        return withCurrentExpectedVersion(current);
      }
      if (!Objects.equals(destination.getExpectedVersion(), current.getVersion())) {
        throw conflict("Destination locator revision is stale");
      }
      ReplicationDestination updated =
          destination
              .toBuilder()
              .version(current.getVersion() + 1)
              .expectedVersion(current.getVersion() + 1)
              .build();
      save(destinationRepository, key, updated, currentRow.get());
      return updated;
    }
    if (!Objects.equals(destination.getExpectedVersion(), 0L)) {
      throw conflict("Destination locator does not exist at the expected revision");
    }
    ReplicationDestination created =
        destination.toBuilder().version(1L).expectedVersion(1L).build();
    save(destinationRepository, key, created, null);
    return created;
  }

  @Override
  public ReplicationCheckpoint findCheckpoint(
      String sourceClusterId,
      String sourceTableUUID,
      long sourceCreationTime,
      String destinationClusterId,
      String destinationTableUUID,
      long destinationCreationTime) {
    ReplicationStateIcebergRowPrimaryKey key =
        destinationKey(
            sourceClusterId,
            sourceTableUUID,
            sourceCreationTime,
            destinationClusterId,
            destinationTableUUID,
            destinationCreationTime);
    return findCheckpointOptional(key)
        .map(row -> readCheckpoint(row.getPayload()))
        .orElseThrow(
            () ->
                new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "Replication checkpoint not found"));
  }

  @Override
  public ReplicationCheckpoint advanceCheckpoint(ReplicationCheckpointUpdate update) {
    ReplicationStateIcebergRowPrimaryKey key = destinationKey(update);
    Optional<ReplicationStateIcebergRow> destinationRow = find(destinationRepository, key);
    if (!destinationRow.isPresent()) {
      throw conflict("Register the replication destination before advancing its checkpoint");
    }
    ReplicationDestination destination = readDestination(destinationRow.get().getPayload());
    if (!sameLocators(destination, update)) {
      throw conflict("Replication locators changed before checkpoint advancement");
    }

    Optional<ReplicationStateIcebergRow> currentRow = find(checkpointRepository, key);
    if (currentRow.isPresent()) {
      ReplicationCheckpoint current = readCheckpoint(currentRow.get().getPayload());
      if (sameCheckpoint(current, update)) {
        return current;
      }
      if (!Objects.equals(update.getExpectedRevision(), current.getRevision())) {
        throw conflict("Replication checkpoint revision is stale");
      }
      ReplicationCheckpoint updated = checkpointFromUpdate(update, current.getRevision() + 1);
      save(checkpointRepository, key, updated, currentRow.get());
      return updated;
    }
    if (!Objects.equals(update.getExpectedRevision(), 0L)) {
      throw conflict("Replication checkpoint does not exist at the expected revision");
    }
    ReplicationCheckpoint created = checkpointFromUpdate(update, 1L);
    save(checkpointRepository, key, created, null);
    return created;
  }

  private Optional<ReplicationStateIcebergRow> find(
      IcebergHtsRepository<ReplicationStateIcebergRow, ReplicationStateIcebergRowPrimaryKey>
          repository,
      ReplicationStateIcebergRowPrimaryKey key) {
    return repository.findById(key);
  }

  private Optional<ReplicationStateIcebergRow> findCheckpointOptional(
      ReplicationStateIcebergRowPrimaryKey key) {
    return find(checkpointRepository, key);
  }

  private void save(
      IcebergHtsRepository<ReplicationStateIcebergRow, ReplicationStateIcebergRowPrimaryKey>
          repository,
      ReplicationStateIcebergRowPrimaryKey key,
      Object payload,
      ReplicationStateIcebergRow current) {
    try {
      repository.save(
          ReplicationStateIcebergRow.builder()
              .sourceClusterId(key.getSourceClusterId())
              .sourceTableUUID(key.getSourceTableUUID())
              .sourceCreationTime(key.getSourceCreationTime())
              .destinationClusterId(key.getDestinationClusterId())
              .destinationTableUUID(key.getDestinationTableUUID())
              .destinationCreationTime(key.getDestinationCreationTime())
              .payload(writePayload(payload))
              .rowVersion(current == null ? null : current.getRowVersion())
              .build());
    } catch (CommitFailedException e) {
      throw conflict("Replication state changed concurrently");
    }
  }

  private ReplicationStateIcebergRowPrimaryKey keyFromRow(ReplicationStateIcebergRow row) {
    return ReplicationStateIcebergRowPrimaryKey.builder()
        .sourceClusterId(row.getSourceClusterId())
        .sourceTableUUID(row.getSourceTableUUID())
        .sourceCreationTime(row.getSourceCreationTime())
        .destinationClusterId(row.getDestinationClusterId())
        .destinationTableUUID(row.getDestinationTableUUID())
        .destinationCreationTime(row.getDestinationCreationTime())
        .build();
  }

  private ReplicationStateIcebergRowPrimaryKey destinationKey(
      String sourceClusterId,
      String sourceTableUUID,
      long sourceCreationTime,
      String destinationClusterId,
      String destinationTableUUID,
      long destinationCreationTime) {
    return ReplicationStateIcebergRowPrimaryKey.builder()
        .sourceClusterId(normalizeCluster(sourceClusterId))
        .sourceTableUUID(normalizeUuid(sourceTableUUID))
        .sourceCreationTime(sourceCreationTime)
        .destinationClusterId(normalizeCluster(destinationClusterId))
        .destinationTableUUID(normalizeUuid(destinationTableUUID))
        .destinationCreationTime(destinationCreationTime)
        .build();
  }

  private ReplicationStateIcebergRowPrimaryKey destinationKey(ReplicationDestination destination) {
    return destinationKey(
        destination.getSourceClusterId(),
        destination.getSourceTableUUID(),
        destination.getSourceCreationTime(),
        destination.getDestinationClusterId(),
        destination.getDestinationTableUUID(),
        destination.getDestinationCreationTime());
  }

  private ReplicationStateIcebergRowPrimaryKey destinationKey(ReplicationCheckpointUpdate update) {
    return destinationKey(
        update.getSourceClusterId(),
        update.getSourceTableUUID(),
        update.getSourceCreationTime(),
        update.getDestinationClusterId(),
        update.getDestinationTableUUID(),
        update.getDestinationCreationTime());
  }

  private static ReplicationCheckpoint checkpointFromUpdate(
      ReplicationCheckpointUpdate update, long revision) {
    return ReplicationCheckpoint.builder()
        .sourceClusterId(update.getSourceClusterId())
        .sourceTableUUID(update.getSourceTableUUID())
        .sourceCreationTime(update.getSourceCreationTime())
        .destinationClusterId(update.getDestinationClusterId())
        .destinationTableUUID(update.getDestinationTableUUID())
        .destinationCreationTime(update.getDestinationCreationTime())
        .sourceTableVersion(update.getSourceTableVersion())
        .sourceSnapshotId(update.getSourceSnapshotId())
        .destinationSnapshotId(update.getDestinationSnapshotId())
        .destinationTableVersion(update.getDestinationTableVersion())
        .revision(revision)
        .build();
  }

  private static ReplicationDestination withCurrentExpectedVersion(
      ReplicationDestination destination) {
    return destination.toBuilder().expectedVersion(destination.getVersion()).build();
  }

  private static boolean sameLocators(
      ReplicationDestination current, ReplicationDestination requested) {
    return sameLocator(current.getSourceDatabaseId(), requested.getSourceDatabaseId())
        && sameLocator(current.getSourceTableId(), requested.getSourceTableId())
        && sameLocator(current.getDestinationDatabaseId(), requested.getDestinationDatabaseId())
        && sameLocator(current.getDestinationTableId(), requested.getDestinationTableId());
  }

  private static boolean sameLocators(
      ReplicationDestination current, ReplicationCheckpointUpdate requested) {
    return sameLocator(current.getSourceDatabaseId(), requested.getSourceDatabaseId())
        && sameLocator(current.getSourceTableId(), requested.getSourceTableId())
        && sameLocator(current.getDestinationDatabaseId(), requested.getDestinationDatabaseId())
        && sameLocator(current.getDestinationTableId(), requested.getDestinationTableId());
  }

  private static boolean sameLocator(String current, String requested) {
    return current != null && requested != null && current.equalsIgnoreCase(requested);
  }

  private static boolean sameCheckpoint(
      ReplicationCheckpoint current, ReplicationCheckpointUpdate requested) {
    return Objects.equals(current.getSourceTableVersion(), requested.getSourceTableVersion())
        && Objects.equals(current.getSourceSnapshotId(), requested.getSourceSnapshotId())
        && Objects.equals(current.getDestinationSnapshotId(), requested.getDestinationSnapshotId())
        && Objects.equals(
            current.getDestinationTableVersion(), requested.getDestinationTableVersion());
  }

  private String writePayload(Object value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unable to serialize replication state", e);
    }
  }

  private ReplicationDestination readDestination(String payload) {
    JsonNode json = readPayload(payload);
    return ReplicationDestination.builder()
        .sourceClusterId(text(json, "sourceClusterId"))
        .sourceTableUUID(text(json, "sourceTableUUID"))
        .sourceCreationTime(number(json, "sourceCreationTime"))
        .sourceDatabaseId(text(json, "sourceDatabaseId"))
        .sourceTableId(text(json, "sourceTableId"))
        .destinationClusterId(text(json, "destinationClusterId"))
        .destinationTableUUID(text(json, "destinationTableUUID"))
        .destinationCreationTime(number(json, "destinationCreationTime"))
        .destinationDatabaseId(text(json, "destinationDatabaseId"))
        .destinationTableId(text(json, "destinationTableId"))
        .expectedVersion(number(json, "expectedVersion"))
        .version(number(json, "version"))
        .build();
  }

  private ReplicationCheckpoint readCheckpoint(String payload) {
    JsonNode json = readPayload(payload);
    return ReplicationCheckpoint.builder()
        .sourceClusterId(text(json, "sourceClusterId"))
        .sourceTableUUID(text(json, "sourceTableUUID"))
        .sourceCreationTime(number(json, "sourceCreationTime"))
        .destinationClusterId(text(json, "destinationClusterId"))
        .destinationTableUUID(text(json, "destinationTableUUID"))
        .destinationCreationTime(number(json, "destinationCreationTime"))
        .sourceTableVersion(text(json, "sourceTableVersion"))
        .sourceSnapshotId(number(json, "sourceSnapshotId"))
        .destinationSnapshotId(number(json, "destinationSnapshotId"))
        .destinationTableVersion(text(json, "destinationTableVersion"))
        .revision(number(json, "revision"))
        .build();
  }

  private JsonNode readPayload(String payload) {
    try {
      return objectMapper.readTree(payload);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to deserialize replication state", e);
    }
  }

  private static String text(JsonNode json, String property) {
    JsonNode value = json.get(property);
    return value == null || value.isNull() ? null : value.asText();
  }

  private static Long number(JsonNode json, String property) {
    JsonNode value = json.get(property);
    return value == null || value.isNull() ? null : value.longValue();
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
}
