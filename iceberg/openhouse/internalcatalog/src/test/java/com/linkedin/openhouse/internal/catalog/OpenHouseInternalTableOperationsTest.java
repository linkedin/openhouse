package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapper;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryTimeoutException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryUnavailableException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class OpenHouseInternalTableOperationsTest {
  private static final String TEST_LOCATION = "test_location";
  private static final TableIdentifier TEST_TABLE_IDENTIFIER =
      TableIdentifier.of("test_db", "test_table");
  private static final TableMetadata BASE_TABLE_METADATA =
      TableMetadata.newTableMetadata(
          new Schema(
              Types.NestedField.required(1, "data", Types.StringType.get()),
              Types.NestedField.required(2, "ts", Types.TimestampType.withoutZone())),
          PartitionSpec.unpartitioned(),
          getTempLocation(),
          ImmutableMap.of());
  @Mock private HouseTableRepository mockHouseTableRepository;
  @Mock private HouseTableMapper mockHouseTableMapper;
  @Mock private HouseTable mockHouseTable;
  @Captor private ArgumentCaptor<TableMetadata> tblMetadataCaptor;

  private OpenHouseInternalTableOperations openHouseInternalTableOperations;

  @SneakyThrows
  private static String getTempLocation() {
    return Files.createTempDirectory(UUID.randomUUID().toString()).toString();
  }

  @BeforeEach
  void setup() {
    MockitoAnnotations.openMocks(this);
    Mockito.when(mockHouseTableMapper.toHouseTable(Mockito.any(TableMetadata.class)))
        .thenReturn(mockHouseTable);
    openHouseInternalTableOperations =
        new OpenHouseInternalTableOperations(
            mockHouseTableRepository,
            new HadoopFileIO(new Configuration()),
            Mockito.mock(SnapshotInspector.class),
            mockHouseTableMapper,
            TEST_TABLE_IDENTIFIER,
            new MetricsReporter(new SimpleMeterRegistry(), "TEST_CATALOG", Lists.newArrayList()));
  }

  @Test
  void testDoCommitAppendSnapshotsInitialVersion() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    Map<String, String> properties = new HashMap<>(BASE_TABLE_METADATA.properties());
    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      properties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(testSnapshots));

      TableMetadata metadata = BASE_TABLE_METADATA.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(BASE_TABLE_METADATA, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture());

      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();
      Assertions.assertEquals(
          4,
          updatedProperties.size()); /*location, lastModifiedTime, version and deleted_snapshots*/
      Assertions.assertEquals(
          "INITIAL_VERSION", updatedProperties.get(getCanonicalFieldName("tableVersion")));
      Assertions.assertEquals(
          testSnapshots.stream()
              .map(s -> Long.toString(s.snapshotId()))
              .collect(Collectors.joining(",")),
          updatedProperties.get(getCanonicalFieldName("appended_snapshots")));
      Assertions.assertTrue(updatedProperties.containsKey(getCanonicalFieldName("tableLocation")));
      Mockito.verify(mockHouseTableRepository, Mockito.times(1)).save(Mockito.eq(mockHouseTable));
    }
  }

  @Test
  void testDoCommitAppendSnapshotsExistingVersion() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    // add 1 snapshot to the base metadata
    TableMetadata base =
        TableMetadata.buildFrom(BASE_TABLE_METADATA)
            .setBranchSnapshot(testSnapshots.get(0), SnapshotRef.MAIN_BRANCH)
            .build();
    Map<String, String> properties = new HashMap<>(base.properties());
    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      // add all snapshots to new metadata
      properties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(testSnapshots));
      properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);

      TableMetadata metadata = base.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(base, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture());

      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();
      Assertions.assertEquals(
          4,
          updatedProperties.size()); /*location, lastModifiedTime, version and deleted_snapshots*/
      Assertions.assertEquals(
          TEST_LOCATION, updatedProperties.get(getCanonicalFieldName("tableVersion")));

      // verify only 3 snapshots are added
      Assertions.assertEquals(
          testSnapshots.subList(1, 4).stream()
              .map(s -> Long.toString(s.snapshotId()))
              .collect(Collectors.joining(",")),
          updatedProperties.get(getCanonicalFieldName("appended_snapshots")));
      Assertions.assertTrue(updatedProperties.containsKey(getCanonicalFieldName("tableLocation")));
      Mockito.verify(mockHouseTableRepository, Mockito.times(1)).save(Mockito.eq(mockHouseTable));
    }
  }

  @Test
  void testDoCommitAppendAndDeleteSnapshots() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    List<Snapshot> extraTestSnapshots = IcebergTestUtil.getExtraSnapshots();
    // add all snapshots to the base metadata
    TableMetadata base = BASE_TABLE_METADATA;
    for (Snapshot snapshot : testSnapshots) {
      base =
          TableMetadata.buildFrom(base)
              .setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH)
              .build();
    }
    Map<String, String> properties = new HashMap<>(base.properties());
    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      // all only last 2 snapshots to new metadata
      List<Snapshot> newSnapshots = new ArrayList<>();
      newSnapshots.addAll(testSnapshots.subList(2, 4));
      newSnapshots.addAll(extraTestSnapshots);
      properties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(newSnapshots));
      properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);

      TableMetadata metadata = base.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(base, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture());

      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();
      Assertions.assertEquals(
          5,
          updatedProperties.size()); /*location, lastModifiedTime, version and deleted_snapshots*/
      Assertions.assertEquals(
          TEST_LOCATION, updatedProperties.get(getCanonicalFieldName("tableVersion")));

      // verify only 4 snapshots are added
      Assertions.assertEquals(
          extraTestSnapshots.stream()
              .map(s -> Long.toString(s.snapshotId()))
              .collect(Collectors.joining(",")),
          updatedProperties.get(getCanonicalFieldName("appended_snapshots")));

      // verify 2 snapshots are deleted
      Assertions.assertEquals(
          testSnapshots.subList(0, 2).stream()
              .map(s -> Long.toString(s.snapshotId()))
              .collect(Collectors.joining(",")),
          updatedProperties.get(getCanonicalFieldName("deleted_snapshots")));
      Assertions.assertTrue(updatedProperties.containsKey(getCanonicalFieldName("tableLocation")));
      Mockito.verify(mockHouseTableRepository, Mockito.times(1)).save(Mockito.eq(mockHouseTable));
    }
  }

  @Test
  void testDoCommitDeleteSnapshots() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    // add all snapshots to the base metadata
    TableMetadata base = BASE_TABLE_METADATA;
    for (Snapshot snapshot : testSnapshots) {
      base =
          TableMetadata.buildFrom(base)
              .setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH)
              .build();
    }
    Map<String, String> properties = new HashMap<>(base.properties());
    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      // all only last 2 snapshots to new metadata
      properties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY,
          SnapshotsUtil.serializedSnapshots(testSnapshots.subList(2, 4)));
      properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);

      TableMetadata metadata = base.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(base, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture());

      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();
      Assertions.assertEquals(
          4,
          updatedProperties.size()); /*location, lastModifiedTime, version and deleted_snapshots*/
      Assertions.assertEquals(
          TEST_LOCATION, updatedProperties.get(getCanonicalFieldName("tableVersion")));

      // verify 2 snapshots are deleted
      Assertions.assertEquals(
          testSnapshots.subList(0, 2).stream()
              .map(s -> Long.toString(s.snapshotId()))
              .collect(Collectors.joining(",")),
          updatedProperties.get(getCanonicalFieldName("deleted_snapshots")));
      Assertions.assertTrue(updatedProperties.containsKey(getCanonicalFieldName("tableLocation")));
      Mockito.verify(mockHouseTableRepository, Mockito.times(1)).save(Mockito.eq(mockHouseTable));
    }
  }

  @Test
  void testDoCommitDoesntPersistForStagedTable() {
    TableMetadata metadata =
        BASE_TABLE_METADATA.replaceProperties(
            ImmutableMap.of(CatalogConstants.IS_STAGE_CREATE_KEY, "true"));
    openHouseInternalTableOperations.commit(null, metadata);
    // Assert TableMetadata is already set for TableOperations
    Assertions.assertNotNull(openHouseInternalTableOperations.currentMetadataLocation());
    Assertions.assertNotNull(openHouseInternalTableOperations.current());
    // Assert houseTableRepository.save() was not called for doCommit()
    verify(mockHouseTableRepository, times(0)).save(null);
    // Assert houseTableRepository.findById() was not called for doRefresh()
    verify(mockHouseTableRepository, times(0)).findById(null);

    Assertions.assertFalse(
        DynFields.builder()
            .hiddenImpl(BaseMetastoreTableOperations.class, "shouldRefresh")
            .<Boolean>build(openHouseInternalTableOperations)
            .get());
  }

  @Test
  void testDoCommitExceptionHandling() {
    TableMetadata base = BASE_TABLE_METADATA;
    TableMetadata metadata =
        BASE_TABLE_METADATA.replaceProperties(ImmutableMap.of("random", "value"));

    when(mockHouseTableRepository.save(Mockito.any(HouseTable.class)))
        .thenThrow(HouseTableCallerException.class);
    Assertions.assertThrows(
        CommitFailedException.class,
        () -> openHouseInternalTableOperations.doCommit(base, metadata));
    when(mockHouseTableRepository.save(Mockito.any(HouseTable.class)))
        .thenThrow(HouseTableConcurrentUpdateException.class);
    Assertions.assertThrows(
        CommitFailedException.class,
        () -> openHouseInternalTableOperations.doCommit(base, metadata));
    when(mockHouseTableRepository.save(Mockito.any(HouseTable.class)))
        .thenThrow(HouseTableNotFoundException.class);
    Assertions.assertThrows(
        CommitFailedException.class,
        () -> openHouseInternalTableOperations.doCommit(base, metadata));
    when(mockHouseTableRepository.save(Mockito.any(HouseTable.class)))
        .thenThrow(HouseTableRepositoryUnavailableException.class);
    Assertions.assertThrows(
        CommitFailedException.class,
        () -> openHouseInternalTableOperations.doCommit(base, metadata));
    when(mockHouseTableRepository.save(Mockito.any(HouseTable.class)))
        .thenThrow(HouseTableRepositoryTimeoutException.class);
    Assertions.assertThrows(
        CommitStateUnknownException.class,
        () -> openHouseInternalTableOperations.doCommit(base, metadata));
  }

  @Test
  void testDoCommitSnapshotsValidationExceptionHandling() throws IOException {
    TableMetadata metadata =
        BASE_TABLE_METADATA.replaceProperties(ImmutableMap.of("random", "value"));
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    Map<String, String> properties = new HashMap<>(metadata.properties());
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY,
        SnapshotsUtil.serializedSnapshots(testSnapshots.subList(1, 3)));
    properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);
    metadata = metadata.replaceProperties(properties);
    TableMetadata metadataWithSnapshots =
        TableMetadata.buildFrom(metadata)
            .setBranchSnapshot(testSnapshots.get(0), SnapshotRef.MAIN_BRANCH)
            .setBranchSnapshot(testSnapshots.get(1), SnapshotRef.MAIN_BRANCH)
            .build();
    TableMetadata metadataWithSnapshotsDeleted =
        TableMetadata.buildFrom(metadata)
            .setBranchSnapshot(testSnapshots.get(3), SnapshotRef.MAIN_BRANCH)
            .build();

    Assertions.assertDoesNotThrow(
        () ->
            openHouseInternalTableOperations.doCommit(
                metadataWithSnapshots, metadataWithSnapshotsDeleted));
  }
}
