package com.linkedin.openhouse.jobs.spark.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.jobs.spark.replication.ReferenceReplicationDataPlane.CopyResult;
import com.linkedin.openhouse.jobs.spark.replication.ReferenceReplicationDataPlane.TableGeneration;
import com.linkedin.openhouse.tables.client.api.ReplicationStateControllerApi;
import com.linkedin.openhouse.tables.client.api.TableApi;
import com.linkedin.openhouse.tables.client.model.GetTableResponseBody;
import com.linkedin.openhouse.tables.client.model.ReplicationCheckpoint;
import com.linkedin.openhouse.tables.client.model.ReplicationCheckpointUpdate;
import com.linkedin.openhouse.tables.client.model.ReplicationDestination;
import com.linkedin.openhouse.tables.client.model.ReplicationEdgeState;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class ReferenceReplicatorSparkAppTest {
  private static final String SOURCE_CLUSTER = "source-cluster";
  private static final String DESTINATION_CLUSTER = "destination-cluster";
  private static final String SOURCE_CATALOG = "source";
  private static final String DESTINATION_CATALOG = "destination";
  private static final String DATABASE = "db";

  @Test
  void replicatesInterleavedWritesAndFollowsSourceRename() {
    TableIdentifier source = TableIdentifier.of(DATABASE, "source_table");
    TableIdentifier renamedSource = TableIdentifier.of(DATABASE, "source_table_renamed");
    TableIdentifier destination = TableIdentifier.of(DATABASE, source.name());
    TableGeneration sourceGeneration = new TableGeneration(UUID.randomUUID().toString(), 100L);
    TableGeneration destinationGeneration = new TableGeneration(UUID.randomUUID().toString(), 200L);
    Map<String, GetTableResponseBody> sourceTables = new HashMap<>();
    Map<String, GetTableResponseBody> destinationTables = new HashMap<>();
    sourceTables.put(
        locator(source),
        tableMetadata(
            SOURCE_CLUSTER,
            sourceGeneration,
            source,
            GetTableResponseBody.TableTypeEnum.PRIMARY_TABLE,
            "source-v0"));
    destinationTables.put(
        locator(destination),
        tableMetadata(
            DESTINATION_CLUSTER,
            destinationGeneration,
            destination,
            GetTableResponseBody.TableTypeEnum.REPLICA_TABLE,
            "destination-v0"));

    TableApi sourceTableApi = mock(TableApi.class);
    TableApi destinationTableApi = mock(TableApi.class);
    ReplicationStateControllerApi replicationApi = mock(ReplicationStateControllerApi.class);
    when(sourceTableApi.getTableV1(anyString(), anyString()))
        .thenAnswer(
            invocation ->
                metadata(sourceTables, invocation.getArgument(0), invocation.getArgument(1)));
    when(destinationTableApi.getTableV1(anyString(), anyString()))
        .thenAnswer(
            invocation ->
                metadata(destinationTables, invocation.getArgument(0), invocation.getArgument(1)));

    AtomicReference<ReplicationDestination> storedDestination =
        new AtomicReference<>(
            destinationAssociation(
                sourceGeneration, destinationGeneration, source, destination, 1L));
    AtomicReference<ReplicationCheckpoint> storedCheckpoint = new AtomicReference<>();
    when(replicationApi.getDestinationsV1(anyString(), anyString(), anyLong()))
        .thenAnswer(
            invocation ->
                Flux.just(
                    new ReplicationEdgeState()
                        .destination(storedDestination.get())
                        .checkpoint(storedCheckpoint.get())));
    when(replicationApi.putDestinationV1(any(ReplicationDestination.class)))
        .thenAnswer(
            invocation -> {
              ReplicationDestination submitted = invocation.getArgument(0);
              long newVersion = storedDestination.get().getVersion() + 1L;
              ReplicationDestination updated =
                  destinationAssociation(
                      sourceGeneration,
                      destinationGeneration,
                      TableIdentifier.of(
                          submitted.getSourceDatabaseId(), submitted.getSourceTableId()),
                      TableIdentifier.of(
                          submitted.getDestinationDatabaseId(), submitted.getDestinationTableId()),
                      newVersion);
              storedDestination.set(updated);
              return Mono.just(updated);
            });
    when(replicationApi.getCheckpointV1(
            anyString(), anyString(), anyLong(), anyString(), anyString(), anyLong()))
        .thenAnswer(
            invocation -> {
              ReplicationCheckpoint checkpoint = storedCheckpoint.get();
              if (checkpoint == null) {
                return Mono.error(
                    new WebClientResponseException(
                        404, "Not Found", HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8));
              }
              return Mono.just(checkpoint);
            });
    when(replicationApi.advanceCheckpointV1(any(ReplicationCheckpointUpdate.class)))
        .thenAnswer(
            invocation -> {
              ReplicationCheckpointUpdate update = invocation.getArgument(0);
              ReplicationCheckpoint checkpoint =
                  new ReplicationCheckpoint()
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
                      .revision(update.getExpectedRevision() + 1L);
              storedCheckpoint.set(checkpoint);
              return Mono.just(checkpoint);
            });

    TestDataPlane dataPlane =
        new TestDataPlane(
            SOURCE_CATALOG,
            DESTINATION_CATALOG,
            source,
            destination,
            sourceGeneration,
            destinationGeneration,
            sourceTables,
            destinationTables);
    ReferenceReplicatorSparkApp app =
        new ReferenceReplicatorSparkApp(
            new ReferenceReplicatorSparkApp.Config(
                SOURCE_CLUSTER,
                sourceGeneration.getTableUuid(),
                sourceGeneration.getCreationTime(),
                SOURCE_CATALOG,
                "http://source",
                DESTINATION_CLUSTER,
                DESTINATION_CATALOG,
                "http://destination",
                "test-token"),
            dataPlane,
            replicationApi,
            sourceTableApi,
            destinationTableApi);

    for (long id = 1; id <= 3; id++) {
      dataPlane.writeSourceRow(id);
      app.run();
      assertEquals(Arrays.asList(1L, 2L, 3L).subList(0, (int) id), dataPlane.destinationRows());
    }

    dataPlane.renameSource(renamedSource);
    dataPlane.writeSourceRow(4L);
    app.run();

    assertEquals(renamedSource, dataPlane.currentSource());
    assertEquals(renamedSource, dataPlane.currentDestination());
    assertEquals(List.of(1L, 2L, 3L, 4L), dataPlane.destinationRows());
    assertFalse(destinationTables.containsKey(locator(destination)));
    assertTrue(destinationTables.containsKey(locator(renamedSource)));
    assertEquals(
        destinationGeneration, dataPlane.getGeneration(DESTINATION_CATALOG, renamedSource));

    ReplicationDestination finalDestination = storedDestination.get();
    assertEquals(renamedSource.name(), finalDestination.getSourceTableId());
    assertEquals(renamedSource.name(), finalDestination.getDestinationTableId());
    assertEquals(DATABASE, finalDestination.getDestinationDatabaseId());

    ReplicationCheckpoint checkpoint = storedCheckpoint.get();
    assertNotNull(checkpoint);
    assertEquals(4L, checkpoint.getRevision());
    assertEquals("source-v4", checkpoint.getSourceTableVersion());
    assertEquals(dataPlane.sourceSnapshotId(), checkpoint.getSourceSnapshotId());
    assertEquals(dataPlane.destinationSnapshotId(), checkpoint.getDestinationSnapshotId());
    assertEquals("destination-v4", checkpoint.getDestinationTableVersion());

    app.run();
    assertEquals(4L, storedCheckpoint.get().getRevision());
    assertEquals(4L, dataPlane.destinationSnapshotId());
  }

  private static ReplicationDestination destinationAssociation(
      TableGeneration sourceGeneration,
      TableGeneration destinationGeneration,
      TableIdentifier source,
      TableIdentifier destination,
      long version) {
    return new ReplicationDestination()
        .sourceClusterId(SOURCE_CLUSTER)
        .sourceTableUUID(sourceGeneration.getTableUuid())
        .sourceCreationTime(sourceGeneration.getCreationTime())
        .sourceDatabaseId(source.namespace().level(0))
        .sourceTableId(source.name())
        .destinationClusterId(DESTINATION_CLUSTER)
        .destinationTableUUID(destinationGeneration.getTableUuid())
        .destinationCreationTime(destinationGeneration.getCreationTime())
        .destinationDatabaseId(destination.namespace().level(0))
        .destinationTableId(destination.name())
        .expectedVersion(version)
        .version(version);
  }

  private static GetTableResponseBody tableMetadata(
      String clusterId,
      TableGeneration generation,
      TableIdentifier identifier,
      GetTableResponseBody.TableTypeEnum tableType,
      String tableVersion) {
    GetTableResponseBody metadata = mock(GetTableResponseBody.class);
    when(metadata.getClusterId()).thenReturn(clusterId);
    when(metadata.getTableUUID()).thenReturn(generation.getTableUuid());
    when(metadata.getCreationTime()).thenReturn(generation.getCreationTime());
    when(metadata.getDatabaseId()).thenReturn(identifier.namespace().level(0));
    when(metadata.getTableId()).thenReturn(identifier.name());
    when(metadata.getTableType()).thenReturn(tableType);
    when(metadata.getTableVersion()).thenReturn(tableVersion);
    return metadata;
  }

  private static Mono<GetTableResponseBody> metadata(
      Map<String, GetTableResponseBody> tables, String database, String table) {
    GetTableResponseBody response = tables.get(database + "." + table);
    return response == null ? Mono.empty() : Mono.just(response);
  }

  private static String locator(TableIdentifier identifier) {
    return identifier.namespace().level(0) + "." + identifier.name();
  }

  private static final class TestDataPlane extends ReferenceReplicationDataPlane {
    private final String sourceCatalogName;
    private final String destinationCatalogName;
    private final TableGeneration sourceGeneration;
    private final TableGeneration destinationGeneration;
    private final Map<String, GetTableResponseBody> sourceTables;
    private final Map<String, GetTableResponseBody> destinationTables;
    private final List<Long> sourceRows = new ArrayList<>();
    private List<Long> destinationRows = new ArrayList<>();
    private TableIdentifier sourceIdentifier;
    private TableIdentifier destinationIdentifier;
    private long sourceSnapshotId;
    private long destinationSnapshotId;

    private TestDataPlane(
        String sourceCatalogName,
        String destinationCatalogName,
        TableIdentifier sourceIdentifier,
        TableIdentifier destinationIdentifier,
        TableGeneration sourceGeneration,
        TableGeneration destinationGeneration,
        Map<String, GetTableResponseBody> sourceTables,
        Map<String, GetTableResponseBody> destinationTables) {
      super(null, (catalogName, identifier) -> null);
      this.sourceCatalogName = sourceCatalogName;
      this.destinationCatalogName = destinationCatalogName;
      this.sourceIdentifier = sourceIdentifier;
      this.destinationIdentifier = destinationIdentifier;
      this.sourceGeneration = sourceGeneration;
      this.destinationGeneration = destinationGeneration;
      this.sourceTables = sourceTables;
      this.destinationTables = destinationTables;
    }

    private void writeSourceRow(long id) {
      sourceRows.add(id);
      sourceSnapshotId++;
      sourceTables.put(
          locator(sourceIdentifier),
          tableMetadata(
              SOURCE_CLUSTER,
              sourceGeneration,
              sourceIdentifier,
              GetTableResponseBody.TableTypeEnum.PRIMARY_TABLE,
              "source-v" + sourceSnapshotId));
    }

    private void renameSource(TableIdentifier renamed) {
      GetTableResponseBody metadata = sourceTables.remove(locator(sourceIdentifier));
      sourceIdentifier = renamed;
      sourceTables.put(
          locator(sourceIdentifier),
          tableMetadata(
              SOURCE_CLUSTER,
              sourceGeneration,
              sourceIdentifier,
              GetTableResponseBody.TableTypeEnum.PRIMARY_TABLE,
              metadata.getTableVersion()));
    }

    private TableIdentifier currentSource() {
      return sourceIdentifier;
    }

    private TableIdentifier currentDestination() {
      return destinationIdentifier;
    }

    private List<Long> destinationRows() {
      return destinationRows;
    }

    private long sourceSnapshotId() {
      return sourceSnapshotId;
    }

    private long destinationSnapshotId() {
      return destinationSnapshotId;
    }

    @Override
    public TableGeneration getGeneration(String catalogName, TableIdentifier identifier) {
      if (sourceCatalogName.equals(catalogName) && sourceIdentifier.equals(identifier)) {
        return sourceGeneration;
      }
      if (destinationCatalogName.equals(catalogName) && destinationIdentifier.equals(identifier)) {
        return destinationGeneration;
      }
      throw new IllegalArgumentException(
          "Unknown table locator: " + catalogName + "." + identifier);
    }

    @Override
    public Optional<TableIdentifier> findTableByGeneration(
        String catalogName, TableGeneration generation) {
      if (sourceCatalogName.equals(catalogName) && sourceGeneration.equals(generation)) {
        return Optional.of(sourceIdentifier);
      }
      if (destinationCatalogName.equals(catalogName) && destinationGeneration.equals(generation)) {
        return Optional.of(destinationIdentifier);
      }
      return Optional.empty();
    }

    @Override
    public long getCurrentSnapshotId(String catalogName, TableIdentifier identifier) {
      if (sourceCatalogName.equals(catalogName)) {
        return sourceSnapshotId;
      }
      if (destinationCatalogName.equals(catalogName)) {
        return destinationSnapshotId;
      }
      throw new IllegalArgumentException("Unknown catalog: " + catalogName);
    }

    @Override
    public OptionalLong findCurrentSnapshotId(String catalogName, TableIdentifier identifier) {
      long snapshotId = getCurrentSnapshotId(catalogName, identifier);
      return snapshotId == 0L ? OptionalLong.empty() : OptionalLong.of(snapshotId);
    }

    @Override
    public void renameReplica(
        String catalogName,
        TableIdentifier from,
        TableIdentifier to,
        TableGeneration expectedGeneration) {
      if (!destinationCatalogName.equals(catalogName)
          || !destinationIdentifier.equals(from)
          || !destinationGeneration.equals(expectedGeneration)) {
        throw new IllegalStateException("Unexpected replica rename request");
      }
      GetTableResponseBody metadata = destinationTables.remove(locator(from));
      destinationIdentifier = to;
      destinationTables.put(
          locator(to),
          tableMetadata(
              DESTINATION_CLUSTER,
              destinationGeneration,
              to,
              GetTableResponseBody.TableTypeEnum.REPLICA_TABLE,
              metadata.getTableVersion()));
    }

    @Override
    public CopyResult copyLatestSnapshot(
        String sourceCatalogName,
        TableIdentifier sourceIdentifier,
        String destinationCatalogName,
        TableIdentifier destinationIdentifier,
        TableGeneration expectedSourceGeneration,
        TableGeneration expectedDestinationGeneration) {
      if (!this.sourceIdentifier.equals(sourceIdentifier)
          || !this.destinationIdentifier.equals(destinationIdentifier)
          || !sourceGeneration.equals(expectedSourceGeneration)
          || !destinationGeneration.equals(expectedDestinationGeneration)) {
        throw new IllegalStateException("Replication requested for a stale table locator");
      }
      destinationRows = new ArrayList<>(sourceRows);
      destinationSnapshotId++;
      destinationTables.put(
          locator(destinationIdentifier),
          tableMetadata(
              DESTINATION_CLUSTER,
              destinationGeneration,
              destinationIdentifier,
              GetTableResponseBody.TableTypeEnum.REPLICA_TABLE,
              "destination-v" + destinationSnapshotId));
      return new CopyResult(sourceSnapshotId, destinationSnapshotId);
    }
  }
}
