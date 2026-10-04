package com.linkedin.openhouse.housetables.repository.impl.iceberg;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.housetables.api.spec.model.ReplicationConfiguration;
import com.linkedin.openhouse.housetables.api.spec.model.ReplicationConfigurationSet;
import com.linkedin.openhouse.hts.catalog.model.replicationconfiguration.ReplicationConfigurationIcebergRow;
import com.linkedin.openhouse.hts.catalog.model.replicationconfiguration.ReplicationConfigurationIcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.model.replicationconfiguration.ReplicationConfigurationStateIcebergRow;
import com.linkedin.openhouse.hts.catalog.model.replicationconfiguration.ReplicationConfigurationStateIcebergRowPrimaryKey;
import com.linkedin.openhouse.hts.catalog.repository.IcebergHtsRepository;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IcebergReplicationConfigurationStoreTest {

  @Mock
  private IcebergHtsRepository<
          ReplicationConfigurationIcebergRow, ReplicationConfigurationIcebergRowPrimaryKey>
      configurationRepository;

  @Mock
  private IcebergHtsRepository<
          ReplicationConfigurationStateIcebergRow,
          ReplicationConfigurationStateIcebergRowPrimaryKey>
      stateRepository;

  private IcebergReplicationConfigurationStore store;

  @BeforeEach
  void setUp() {
    store = new IcebergReplicationConfigurationStore(configurationRepository, stateRepository);
  }

  @Test
  void replaceWritesEdgesBeforePublishingConfiguredState() {
    when(stateRepository.findById(any())).thenReturn(Optional.empty());
    when(configurationRepository.searchByPartialId(any())).thenReturn(Collections.emptyList());
    ReplicationConfigurationSet desired =
        ReplicationConfigurationSet.builder()
            .sourceDatabaseId("source_db")
            .sourceTableId("source_table")
            .configured(true)
            .configurations(
                Arrays.asList(
                    configuration("cluster_a", "db_a", "table_a", "12H"),
                    configuration("cluster_b", "db_b", "table_b", "1D")))
            .build();

    store.replace(desired);

    ArgumentCaptor<ReplicationConfigurationIcebergRowPrimaryKey> partialKey =
        ArgumentCaptor.forClass(ReplicationConfigurationIcebergRowPrimaryKey.class);
    ArgumentCaptor<java.util.List<ReplicationConfigurationIcebergRow>> rows =
        ArgumentCaptor.forClass(java.util.List.class);
    verify(configurationRepository).replaceByPartialId(partialKey.capture(), rows.capture());
    assertEquals("SOURCE_DB", partialKey.getValue().getSourceDatabaseId());
    assertEquals("SOURCE_TABLE", partialKey.getValue().getSourceTableId());
    assertEquals(2, rows.getValue().size());
    assertEquals("CLUSTER_A", rows.getValue().get(0).getDestinationClusterId());
    assertEquals("12H", rows.getValue().get(0).getReplicationInterval());
    assertEquals("CLUSTER_B", rows.getValue().get(1).getDestinationClusterId());
    InOrder writeOrder = inOrder(configurationRepository, stateRepository);
    writeOrder.verify(configurationRepository).replaceByPartialId(any(), anyList());
    writeOrder.verify(stateRepository).save(any(ReplicationConfigurationStateIcebergRow.class));
  }

  @Test
  void findBySourceReadsConfiguredEdgesAndIgnoresEdgesAfterClear() {
    ReplicationConfigurationStateIcebergRow configuredState =
        ReplicationConfigurationStateIcebergRow.builder()
            .sourceDatabaseId("SOURCE_DB")
            .sourceTableId("SOURCE_TABLE")
            .configured(true)
            .build();
    doReturn(Optional.of(configuredState))
        .doReturn(
            Optional.of(
                ReplicationConfigurationStateIcebergRow.builder()
                    .sourceDatabaseId("SOURCE_DB")
                    .sourceTableId("SOURCE_TABLE")
                    .configured(false)
                    .build()))
        .when(stateRepository)
        .findById(any());
    when(configurationRepository.searchByPartialId(any()))
        .thenReturn(
            Collections.singletonList(
                ReplicationConfigurationIcebergRow.builder()
                    .sourceDatabaseId("SOURCE_DB")
                    .sourceTableId("SOURCE_TABLE")
                    .destinationClusterId("CLUSTER_A")
                    .destinationDatabaseId("DB_A")
                    .destinationTableId("TABLE_A")
                    .replicationInterval("12H")
                    .build()));

    ReplicationConfigurationSet found = store.findBySource("source_db", "source_table").get();

    assertTrue(found.getConfigured());
    assertEquals(1, found.getConfigurations().size());
    assertEquals("CLUSTER_A", found.getConfigurations().get(0).getDestinationClusterId());
    assertEquals("12H", found.getConfigurations().get(0).getReplicationInterval());

    ReplicationConfigurationSet cleared = store.findBySource("source_db", "source_table").get();

    assertFalse(cleared.getConfigured());
    assertTrue(cleared.getConfigurations().isEmpty());
    verify(configurationRepository, times(1)).searchByPartialId(any());
  }

  @Test
  void replaceRejectsDuplicateDestinationsIgnoringCase() {
    ReplicationConfigurationSet duplicateDestinations =
        ReplicationConfigurationSet.builder()
            .sourceDatabaseId("source_db")
            .sourceTableId("source_table")
            .configured(true)
            .configurations(
                Arrays.asList(
                    configuration("cluster_a", "db_a", "table_a", "12H"),
                    configuration("CLUSTER_A", "DB_A", "TABLE_A", "1D")))
            .build();

    assertThrows(IllegalArgumentException.class, () -> store.replace(duplicateDestinations));
    verify(configurationRepository, never()).replaceByPartialId(any(), anyList());
    verify(stateRepository, never()).save(any(ReplicationConfigurationStateIcebergRow.class));
  }

  private static ReplicationConfiguration configuration(
      String clusterId, String databaseId, String tableId, String interval) {
    return ReplicationConfiguration.builder()
        .destinationClusterId(clusterId)
        .destinationDatabaseId(databaseId)
        .destinationTableId(tableId)
        .replicationInterval(interval)
        .build();
  }
}
