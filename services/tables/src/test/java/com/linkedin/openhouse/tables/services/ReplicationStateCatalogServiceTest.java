package com.linkedin.openhouse.tables.services;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.housetables.client.api.ReplicationStateControllerApi;
import com.linkedin.openhouse.housetables.client.model.ReplicationCheckpointUpdate;
import com.linkedin.openhouse.housetables.client.model.ReplicationDestination;
import com.linkedin.openhouse.housetables.client.model.ReplicationEdgeState;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;

@ExtendWith(MockitoExtension.class)
class ReplicationStateCatalogServiceTest {
  @Mock private ReplicationStateControllerApi replicationStateApi;
  @Mock private TablesService tablesService;
  @Mock private AuthorizationUtils authorizationUtils;

  @InjectMocks private ReplicationStateCatalogService service;

  @Test
  void checkpointDoesNotAdvanceWhenDestinationCommitVersionIsNotCurrent() {
    ReplicationDestination destination =
        new ReplicationDestination()
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
            .expectedVersion(1L);
    when(replicationStateApi.getDestinations("source", "source-uuid", 10L))
        .thenReturn(Flux.just(new ReplicationEdgeState().destination(destination)));
    when(tablesService.getTable("destination-db", "destination-table", "replicator"))
        .thenReturn(
            TableDto.builder()
                .clusterId("destination")
                .tableUUID("destination-uuid")
                .creationTime(20L)
                .tableVersion("current-version")
                .build());
    ReplicationCheckpointUpdate update =
        new ReplicationCheckpointUpdate()
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
            .expectedRevision(0L)
            .sourceTableVersion("source-version")
            .sourceSnapshotId(100L)
            .destinationSnapshotId(200L)
            .destinationTableVersion("commit-response-version");

    assertThatThrownBy(() -> service.advanceCheckpoint(update, "replicator"))
        .isInstanceOf(ResponseStatusException.class)
        .extracting(error -> ((ResponseStatusException) error).getStatus())
        .isEqualTo(HttpStatus.CONFLICT);

    verify(authorizationUtils)
        .checkTablePrivilege(any(TableDto.class), eq("replicator"), eq(Privileges.SYSTEM_ADMIN));
    verify(replicationStateApi, never()).advanceCheckpoint(any());
  }
}
