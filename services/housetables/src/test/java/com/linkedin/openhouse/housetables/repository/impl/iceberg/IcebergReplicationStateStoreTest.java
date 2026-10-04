package com.linkedin.openhouse.housetables.repository.impl.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationCheckpointUpdate;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationDestination;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationEdgeState;
import com.linkedin.openhouse.hts.catalog.model.replication.ReplicationStateIcebergRow;
import com.linkedin.openhouse.hts.catalog.model.replication.ReplicationStateIcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.repository.IcebergHtsRepository;
import java.nio.file.Path;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

class IcebergReplicationStateStoreTest {
  @TempDir Path tempDir;

  @Test
  void destinationAndCheckpointUseIndependentIcebergTablesWithCasRetries() {
    Catalog catalog = createCatalog();
    IcebergReplicationStateStore store =
        new IcebergReplicationStateStore(
            repository(catalog, "replicationDestination"),
            repository(catalog, "replicationCheckpoint"),
            new ObjectMapper());
    ReplicationDestination destination =
        ReplicationDestination.builder()
            .sourceClusterId("source")
            .sourceTableUUID("source-uuid")
            .sourceCreationTime(10L)
            .sourceDatabaseId("source-db")
            .sourceTableId("source-table")
            .destinationClusterId("destination")
            .destinationTableUUID("destination-uuid")
            .destinationCreationTime(20L)
            .destinationDatabaseId("destination-db")
            .destinationTableId("destination-table")
            .expectedVersion(0L)
            .build();

    assertThat(store.putDestination(destination).getVersion()).isEqualTo(1L);
    assertThat(store.putDestination(destination).getVersion()).isEqualTo(1L);

    ReplicationCheckpointUpdate update = checkpointUpdate(0L, 100L);
    assertThat(store.advanceCheckpoint(update).getRevision()).isEqualTo(1L);
    assertThat(store.advanceCheckpoint(update).getRevision()).isEqualTo(1L);
    assertThat(
            store.findDestinations("source", "source-uuid", 10L).stream()
                .map(ReplicationEdgeState::getCheckpoint)
                .findFirst()
                .get()
                .getSourceSnapshotId())
        .isEqualTo(100L);

    assertThatThrownBy(() -> store.advanceCheckpoint(checkpointUpdate(0L, 101L)))
        .isInstanceOf(ResponseStatusException.class)
        .extracting(error -> ((ResponseStatusException) error).getStatus())
        .isEqualTo(HttpStatus.CONFLICT);
  }

  private Catalog createCatalog() {
    HadoopCatalog catalog = new HadoopCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize(
        "replication-test",
        Collections.singletonMap(CatalogProperties.WAREHOUSE_LOCATION, tempDir.toString()));
    catalog.createNamespace(Namespace.of("htsDB"));
    return catalog;
  }

  private static IcebergHtsRepository<
          ReplicationStateIcebergRow, ReplicationStateIcebergRowPrimaryKey>
      repository(Catalog catalog, String tableName) {
    return IcebergHtsRepository
        .<ReplicationStateIcebergRow, ReplicationStateIcebergRowPrimaryKey>builder()
        .catalog(catalog)
        .htsTableIdentifier(TableIdentifier.of("htsDB", tableName))
        .build();
  }

  private static ReplicationCheckpointUpdate checkpointUpdate(
      long expectedRevision, long sourceSnapshotId) {
    return ReplicationCheckpointUpdate.builder()
        .sourceClusterId("source")
        .sourceTableUUID("source-uuid")
        .sourceCreationTime(10L)
        .destinationClusterId("destination")
        .destinationTableUUID("destination-uuid")
        .destinationCreationTime(20L)
        .sourceDatabaseId("source-db")
        .sourceTableId("source-table")
        .destinationDatabaseId("destination-db")
        .destinationTableId("destination-table")
        .expectedRevision(expectedRevision)
        .sourceTableVersion("source-version")
        .sourceSnapshotId(sourceSnapshotId)
        .destinationSnapshotId(200L)
        .destinationTableVersion("destination-version")
        .build();
  }
}
