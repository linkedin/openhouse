package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.housetables.client.api.ReplicationConfigurationApi;
import com.linkedin.openhouse.housetables.client.model.ReplicationConfiguration;
import com.linkedin.openhouse.housetables.client.model.ReplicationConfigurationSet;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Replication;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ReplicationConfigurationCatalogServiceTest {

  @Mock private ReplicationConfigurationApi replicationConfigurationApi;

  @InjectMocks private ReplicationConfigurationCatalogService service;

  @Test
  void synchronizePersistsExistingReplicationContractToCatalog() {
    TableDto requested =
        TableDto.builder()
            .databaseId("source_db")
            .tableId("source_table")
            .policies(
                Policies.builder()
                    .replication(
                        Replication.builder()
                            .config(
                                Collections.singletonList(
                                    ReplicationConfig.builder()
                                        .destination("CLUSTER2")
                                        .interval("12H")
                                        .build()))
                            .build())
                    .build())
            .build();
    when(replicationConfigurationApi.replaceReplicationConfiguration(any()))
        .thenReturn(Mono.empty());

    service.synchronize(requested, null, false);

    ArgumentCaptor<ReplicationConfigurationSet> captor =
        ArgumentCaptor.forClass(ReplicationConfigurationSet.class);
    verify(replicationConfigurationApi).replaceReplicationConfiguration(captor.capture());
    ReplicationConfigurationSet stored = captor.getValue();
    assertEquals("source_db", stored.getSourceDatabaseId());
    assertEquals("source_table", stored.getSourceTableId());
    assertTrue(stored.getConfigured());
    assertEquals(1, stored.getConfigurations().size());
    ReplicationConfiguration configuration = stored.getConfigurations().get(0);
    assertEquals("CLUSTER2", configuration.getDestinationClusterId());
    assertEquals("source_db", configuration.getDestinationDatabaseId());
    assertEquals("source_table", configuration.getDestinationTableId());
    assertEquals("12H", configuration.getReplicationInterval());
  }

  @Test
  void synchronizeClearsCatalogStateWhenReplicationIsOmitted() {
    TableDto existing =
        TableDto.builder()
            .databaseId("source_db")
            .tableId("source_table")
            .policies(Policies.builder().replication(Replication.builder().build()).build())
            .build();
    TableDto requested = TableDto.builder().databaseId("source_db").tableId("source_table").build();
    when(replicationConfigurationApi.getReplicationConfiguration("source_db", "source_table"))
        .thenReturn(
            Mono.just(
                new ReplicationConfigurationSet()
                    .sourceDatabaseId("source_db")
                    .sourceTableId("source_table")
                    .configured(true)
                    .configurations(Collections.emptyList())));
    when(replicationConfigurationApi.replaceReplicationConfiguration(any()))
        .thenReturn(Mono.empty());

    service.synchronize(requested, existing, false);

    ArgumentCaptor<ReplicationConfigurationSet> captor =
        ArgumentCaptor.forClass(ReplicationConfigurationSet.class);
    verify(replicationConfigurationApi).replaceReplicationConfiguration(captor.capture());
    assertFalse(captor.getValue().getConfigured());
    assertTrue(captor.getValue().getConfigurations().isEmpty());
  }

  @Test
  void enrichUsesCatalogConfigurationInsteadOfLegacyMetadata() {
    TableDto legacy =
        TableDto.builder()
            .databaseId("source_db")
            .tableId("source_table")
            .policies(
                Policies.builder()
                    .replication(
                        Replication.builder()
                            .config(
                                Collections.singletonList(
                                    ReplicationConfig.builder()
                                        .destination("LEGACY")
                                        .interval("1D")
                                        .build()))
                            .build())
                    .build())
            .build();
    when(replicationConfigurationApi.getReplicationConfiguration("source_db", "source_table"))
        .thenReturn(
            Mono.just(
                new ReplicationConfigurationSet()
                    .sourceDatabaseId("source_db")
                    .sourceTableId("source_table")
                    .configured(true)
                    .configurations(
                        Collections.singletonList(
                            new ReplicationConfiguration()
                                .destinationClusterId("CATALOG")
                                .destinationDatabaseId("source_db")
                                .destinationTableId("source_table")
                                .replicationInterval("12H")))));

    TableDto enriched = service.enrich(legacy);

    assertEquals(
        "CATALOG", enriched.getPolicies().getReplication().getConfig().get(0).getDestination());
    assertEquals("12H", enriched.getPolicies().getReplication().getConfig().get(0).getInterval());
    verify(replicationConfigurationApi, never()).replaceReplicationConfiguration(any());
  }

  @Test
  void enrichFallsBackToLegacyMetadataWhenCatalogStateDoesNotExist() {
    TableDto legacy =
        TableDto.builder()
            .databaseId("source_db")
            .tableId("source_table")
            .policies(Policies.builder().replication(Replication.builder().build()).build())
            .build();
    when(replicationConfigurationApi.getReplicationConfiguration("source_db", "source_table"))
        .thenReturn(
            Mono.error(
                new WebClientResponseException(
                    HttpStatus.NOT_FOUND.value(),
                    "Not Found",
                    HttpHeaders.EMPTY,
                    new byte[0],
                    null)));

    assertSame(legacy, service.enrich(legacy));
    verify(replicationConfigurationApi, never()).replaceReplicationConfiguration(any());
  }

  @Test
  void synchronizePreservesOmittedReplicationDuringReplaceCommit() {
    TableDto existing =
        TableDto.builder()
            .databaseId("source_db")
            .tableId("source_table")
            .policies(Policies.builder().build())
            .build();
    TableDto requested = TableDto.builder().databaseId("source_db").tableId("source_table").build();

    service.synchronize(requested, existing, true);

    verifyNoInteractions(replicationConfigurationApi);
  }

  @Test
  void synchronizePropagatesCatalogWriteFailure() {
    TableDto requested =
        TableDto.builder()
            .databaseId("source_db")
            .tableId("source_table")
            .policies(
                Policies.builder()
                    .replication(
                        Replication.builder()
                            .config(
                                Collections.singletonList(
                                    ReplicationConfig.builder()
                                        .destination("CLUSTER2")
                                        .interval("12H")
                                        .build()))
                            .build())
                    .build())
            .build();
    RuntimeException failure = new RuntimeException("HTS unavailable");
    when(replicationConfigurationApi.replaceReplicationConfiguration(any()))
        .thenReturn(Mono.error(failure));

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> service.synchronize(requested, null, false));

    assertSame(failure, thrown);
  }
}
