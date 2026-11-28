package com.linkedin.openhouse.tables.services;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Policies;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.Replication;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ReplicationConfig;
import com.linkedin.openhouse.tables.common.TableType;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.server.ResponseStatusException;

public class ReplicationCascadeAssociationResolverTest {

  private OpenHouseInternalRepository repository;
  private ReplicationCascadeAssociationResolver resolver;

  @BeforeEach
  public void setup() {
    repository = mock(OpenHouseInternalRepository.class);
    ClusterProperties clusterProperties = new ClusterProperties();
    ReflectionTestUtils.setField(clusterProperties, "clusterName", "clusterB");
    resolver = new ReplicationCascadeAssociationResolver();
    ReflectionTestUtils.setField(resolver, "repository", repository);
    ReflectionTestUtils.setField(resolver, "clusterProperties", clusterProperties);
  }

  @Test
  public void resolvesReplicaWhosePolicyIncludesReceivingCluster() {
    TableDto replica = replica("clusterA", "clusterB");
    when(repository.findById(any(TableDtoPrimaryKey.class))).thenReturn(Optional.of(replica));

    Assertions.assertEquals(Optional.of(replica), resolver.resolve("db", "replica"));
  }

  @Test
  public void returnsEmptyWhenReplicaDoesNotExist() {
    when(repository.findById(any(TableDtoPrimaryKey.class))).thenReturn(Optional.empty());

    Assertions.assertFalse(resolver.resolve("db", "replica").isPresent());
  }

  @Test
  public void rejectsTableThatIsNotAReplica() {
    TableDto primary = replica("clusterB").toBuilder().tableType(TableType.PRIMARY_TABLE).build();
    when(repository.findById(any(TableDtoPrimaryKey.class))).thenReturn(Optional.of(primary));

    Assertions.assertThrows(ResponseStatusException.class, () -> resolver.resolve("db", "replica"));
  }

  @Test
  public void rejectsReplicaWithoutReceivingClusterInReplicationPolicy() {
    TableDto replica = replica("clusterA");
    when(repository.findById(any(TableDtoPrimaryKey.class))).thenReturn(Optional.of(replica));

    Assertions.assertThrows(ResponseStatusException.class, () -> resolver.resolve("db", "replica"));
  }

  private static TableDto replica(String... destinations) {
    return TableDto.builder()
        .databaseId("db")
        .tableId("replica")
        .tableUUID("replica-uuid")
        .creationTime(123L)
        .tableType(TableType.REPLICA_TABLE)
        .policies(
            Policies.builder()
                .replication(
                    Replication.builder()
                        .config(
                            Arrays.stream(destinations)
                                .map(
                                    destination ->
                                        ReplicationConfig.builder()
                                            .destination(destination)
                                            .build())
                                .collect(java.util.stream.Collectors.toList()))
                        .build())
                .build())
        .build();
  }
}
