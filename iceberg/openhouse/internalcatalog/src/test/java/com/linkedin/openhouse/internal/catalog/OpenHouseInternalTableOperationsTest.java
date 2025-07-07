package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.local.LocalStorage;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapper;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
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
  @Mock private FileIOManager fileIOManager;
  @Mock private MetricsReporter mockMetricsReporter;

  private OpenHouseInternalTableOperations openHouseInternalTableOperations;
  private OpenHouseInternalTableOperations openHouseInternalTableOperationsWithMockMetrics;

  @SneakyThrows
  private static String getTempLocation() {
    return Files.createTempDirectory(UUID.randomUUID().toString()).toString();
  }

  @BeforeEach
  void setup() {
    MockitoAnnotations.openMocks(this);
    Mockito.when(mockHouseTableMapper.toHouseTable(Mockito.any(TableMetadata.class), Mockito.any()))
        .thenReturn(mockHouseTable);
    HadoopFileIO fileIO = new HadoopFileIO(new Configuration());
    openHouseInternalTableOperations =
        new OpenHouseInternalTableOperations(
            mockHouseTableRepository,
            fileIO,
            Mockito.mock(SnapshotInspector.class),
            mockHouseTableMapper,
            TEST_TABLE_IDENTIFIER,
            new MetricsReporter(new SimpleMeterRegistry(), "TEST_CATALOG", Lists.newArrayList()));

    // Create a separate instance with mock metrics reporter for testing metrics
    openHouseInternalTableOperationsWithMockMetrics =
        new OpenHouseInternalTableOperations(
            mockHouseTableRepository,
            fileIO,
            Mockito.mock(SnapshotInspector.class),
            mockHouseTableMapper,
            TEST_TABLE_IDENTIFIER,
            mockMetricsReporter);

    LocalStorage localStorage = mock(LocalStorage.class);
    when(fileIOManager.getStorage(fileIO)).thenReturn(localStorage);
    when(localStorage.getType()).thenReturn(StorageType.LOCAL);
  }

  @Test
  void testDoCommitAppendSnapshotsInitialVersion() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    Map<String, String> properties = new HashMap<>(BASE_TABLE_METADATA.properties());
    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      properties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(testSnapshots));
      properties.put(
          CatalogConstants.SNAPSHOTS_REFS_KEY,
          SnapshotsUtil.serializeMap(
              IcebergTestUtil.obtainSnapshotRefsFromSnapshot(
                  testSnapshots.get(testSnapshots.size() - 1))));

      TableMetadata metadata = BASE_TABLE_METADATA.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(BASE_TABLE_METADATA, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());

      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();
      Assertions.assertEquals(
          5,
          updatedProperties
              .size()); /*write.parquet.compression-codec, location, lastModifiedTime, version and appended_snapshots*/
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
      properties.put(
          CatalogConstants.SNAPSHOTS_REFS_KEY,
          SnapshotsUtil.serializeMap(
              IcebergTestUtil.obtainSnapshotRefsFromSnapshot(
                  testSnapshots.get(testSnapshots.size() - 1))));
      properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);

      TableMetadata metadata = base.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(base, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());

      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();
      Assertions.assertEquals(
          5,
          updatedProperties
              .size()); /*write.parquet.compression-codec, location, lastModifiedTime, version and deleted_snapshots*/
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
      properties.put(
          CatalogConstants.SNAPSHOTS_REFS_KEY,
          SnapshotsUtil.serializeMap(
              IcebergTestUtil.obtainSnapshotRefsFromSnapshot(
                  newSnapshots.get(newSnapshots.size() - 1))));
      properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);

      TableMetadata metadata = base.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(base, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());

      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();
      Assertions.assertEquals(
          6,
          updatedProperties
              .size()); /*write.parquet.compression-codec, location, lastModifiedTime, version, appended_snapshots and deleted_snapshots*/
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
      properties.put(
          CatalogConstants.SNAPSHOTS_REFS_KEY,
          SnapshotsUtil.serializeMap(
              IcebergTestUtil.obtainSnapshotRefsFromSnapshot(
                  testSnapshots.get(testSnapshots.size() - 1))));
      properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);

      TableMetadata metadata = base.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(base, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());

      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();
      Assertions.assertEquals(
          5,
          updatedProperties
              .size()); /*write.parquet.compression-codec, location, lastModifiedTime, version and deleted_snapshots*/
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
        .thenThrow(HouseTableRepositoryStateUnknownException.class);
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
    properties.put(
        CatalogConstants.SNAPSHOTS_REFS_KEY,
        SnapshotsUtil.serializeMap(
            IcebergTestUtil.obtainSnapshotRefsFromSnapshot(
                testSnapshots.get(testSnapshots.size() - 1))));
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

  @Test
  void testDoCommitAppendStageOnlySnapshotsInitialVersion() throws IOException {
    List<Snapshot> testWapSnapshots = IcebergTestUtil.getWapSnapshots().subList(0, 2);
    Map<String, String> properties = new HashMap<>(BASE_TABLE_METADATA.properties());
    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      properties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(testWapSnapshots));
      properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);

      TableMetadata metadata = BASE_TABLE_METADATA.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(BASE_TABLE_METADATA, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());
      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();

      // verify snapshots are staged but not appended
      Assertions.assertEquals(
          testWapSnapshots.stream()
              .map(s -> Long.toString(s.snapshotId()))
              .collect(Collectors.joining(",")),
          updatedProperties.get(getCanonicalFieldName("staged_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("appended_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("cherry_picked_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("deleted_snapshots")));
      Mockito.verify(mockHouseTableRepository, Mockito.times(1)).save(Mockito.eq(mockHouseTable));
    }
  }

  @Test
  void testDoCommitAppendStageOnlySnapshotsExistingVersion() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    List<Snapshot> testWapSnapshots = IcebergTestUtil.getWapSnapshots().subList(0, 2);
    // add 1 snapshot to the base metadata
    TableMetadata base =
        TableMetadata.buildFrom(BASE_TABLE_METADATA)
            .setBranchSnapshot(testSnapshots.get(0), SnapshotRef.MAIN_BRANCH)
            .build();
    List<Snapshot> newSnapshots = new ArrayList<>();
    newSnapshots.add(testSnapshots.get(0));
    newSnapshots.addAll(testWapSnapshots);
    Map<String, String> properties = new HashMap<>(base.properties());
    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      // add staged snapshots to the new metadata
      properties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(newSnapshots));
      properties.put(
          CatalogConstants.SNAPSHOTS_REFS_KEY,
          SnapshotsUtil.serializeMap(
              IcebergTestUtil.obtainSnapshotRefsFromSnapshot(newSnapshots.get(0))));
      properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);

      TableMetadata metadata = base.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(base, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());
      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();

      // verify snapshots are staged but not appended
      Assertions.assertEquals(
          testWapSnapshots.stream()
              .map(s -> Long.toString(s.snapshotId()))
              .collect(Collectors.joining(",")),
          updatedProperties.get(getCanonicalFieldName("staged_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("appended_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("cherry_picked_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("deleted_snapshots")));
      Mockito.verify(mockHouseTableRepository, Mockito.times(1)).save(Mockito.eq(mockHouseTable));
    }
  }

  @Test
  void testDoCommitAppendSnapshotsToNonMainBranch() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    Map<String, String> properties = new HashMap<>(BASE_TABLE_METADATA.properties());
    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      properties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY,
          SnapshotsUtil.serializedSnapshots(testSnapshots.subList(0, 1)));
      properties.put(
          CatalogConstants.SNAPSHOTS_REFS_KEY,
          SnapshotsUtil.serializeMap(
              IcebergTestUtil.obtainSnapshotRefsFromSnapshot(testSnapshots.get(0), "branch")));
      properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);

      TableMetadata metadata = BASE_TABLE_METADATA.replaceProperties(properties);
      // verify throw an error when committing to non-main branch.
      Assertions.assertThrows(
          CommitStateUnknownException.class,
          () -> openHouseInternalTableOperations.doCommit(BASE_TABLE_METADATA, metadata));
    }
  }

  @Test
  void testAppendSnapshotsWithOldSnapshots() throws IOException {
    TableMetadata metadata =
        TableMetadata.buildFrom(BASE_TABLE_METADATA)
            .setPreviousFileLocation("tmp_location")
            .setLocation(BASE_TABLE_METADATA.metadataFileLocation())
            .build();
    // all snapshots are from the past and snapshots add should fail the validation
    List<Snapshot> snapshots = IcebergTestUtil.getSnapshots();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            openHouseInternalTableOperations.maybeAppendSnapshots(
                metadata, snapshots, ImmutableMap.of(), false));
    // the latest snapshots have larger timestamp than the previous metadata timestamp, so it should
    // pass the validation
    snapshots.addAll(IcebergTestUtil.getFutureSnapshots());
    openHouseInternalTableOperations.maybeAppendSnapshots(
        metadata, snapshots, ImmutableMap.of(), false);
  }

  @Test
  void testDoCommitCherryPickSnapshotBaseUnchanged() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    List<Snapshot> testWapSnapshots = IcebergTestUtil.getWapSnapshots();
    // add 1 snapshot and 1 staged snapshot to the base metadata
    TableMetadata base =
        TableMetadata.buildFrom(BASE_TABLE_METADATA)
            .setBranchSnapshot(testSnapshots.get(0), SnapshotRef.MAIN_BRANCH)
            .addSnapshot(testWapSnapshots.get(0))
            .build();
    List<Snapshot> newSnapshots = new ArrayList<>();
    newSnapshots.add(testSnapshots.get(0));
    newSnapshots.add(testWapSnapshots.get(0));
    Map<String, String> properties = new HashMap<>(base.properties());
    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      // cherry pick the staged snapshot
      properties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(newSnapshots));
      properties.put(
          CatalogConstants.SNAPSHOTS_REFS_KEY,
          SnapshotsUtil.serializeMap(
              IcebergTestUtil.obtainSnapshotRefsFromSnapshot(testWapSnapshots.get(0))));
      properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);

      TableMetadata metadata = base.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(base, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());
      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();

      // verify the staged snapshot is cherry picked by use the existing one
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("staged_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("appended_snapshots")));
      Assertions.assertEquals(
          Long.toString(testWapSnapshots.get(0).snapshotId()),
          updatedProperties.get(getCanonicalFieldName("cherry_picked_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("deleted_snapshots")));
      Mockito.verify(mockHouseTableRepository, Mockito.times(1)).save(Mockito.eq(mockHouseTable));
    }
  }

  @Test
  void testDoCommitCherryPickSnapshotBaseChanged() throws IOException {
    List<Snapshot> testWapSnapshots = IcebergTestUtil.getWapSnapshots();
    // add 1 snapshot and 1 staged snapshot to the base metadata
    TableMetadata base =
        TableMetadata.buildFrom(BASE_TABLE_METADATA)
            .setBranchSnapshot(testWapSnapshots.get(0), SnapshotRef.MAIN_BRANCH)
            .addSnapshot(testWapSnapshots.get(1))
            .build();
    Map<String, String> properties = new HashMap<>(base.properties());
    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      // cherry pick the staged snapshot whose base has changed
      properties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(testWapSnapshots));
      properties.put(
          CatalogConstants.SNAPSHOTS_REFS_KEY,
          SnapshotsUtil.serializeMap(
              IcebergTestUtil.obtainSnapshotRefsFromSnapshot(
                  testWapSnapshots.get(2)))); // new snapshot
      properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);

      TableMetadata metadata = base.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(base, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());
      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();

      // verify the staged snapshot is cherry picked by creating a new snapshot and append it
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("staged_snapshots")));
      Assertions.assertEquals(
          Long.toString(testWapSnapshots.get(2).snapshotId()),
          updatedProperties.get(getCanonicalFieldName("appended_snapshots")));
      Assertions.assertEquals(
          Long.toString(testWapSnapshots.get(1).snapshotId()),
          updatedProperties.get(getCanonicalFieldName("cherry_picked_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("deleted_snapshots")));
      Mockito.verify(mockHouseTableRepository, Mockito.times(1)).save(Mockito.eq(mockHouseTable));
    }
  }

  @Test
  void testDoCommitCherryPickFirstSnapshot() throws IOException {
    List<Snapshot> testWapSnapshots = IcebergTestUtil.getWapSnapshots().subList(0, 1);
    // add 1 staged snapshot to the base metadata
    TableMetadata base =
        TableMetadata.buildFrom(BASE_TABLE_METADATA).addSnapshot(testWapSnapshots.get(0)).build();
    Map<String, String> properties = new HashMap<>(base.properties());
    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      // cherry pick the staged snapshot
      properties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(testWapSnapshots));
      properties.put(
          CatalogConstants.SNAPSHOTS_REFS_KEY,
          SnapshotsUtil.serializeMap(
              IcebergTestUtil.obtainSnapshotRefsFromSnapshot(testWapSnapshots.get(0))));
      properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);

      TableMetadata metadata = base.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(base, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());
      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();

      // verify the staged snapshot is cherry picked by using the existing one
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("staged_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("appended_snapshots")));
      Assertions.assertEquals(
          Long.toString(testWapSnapshots.get(0).snapshotId()),
          updatedProperties.get(getCanonicalFieldName("cherry_picked_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("deleted_snapshots")));
      Mockito.verify(mockHouseTableRepository, Mockito.times(1)).save(Mockito.eq(mockHouseTable));
    }
  }

  @Test
  void testDoCommitDeleteLastStagedSnapshotWhenNoRefs() throws IOException {
    List<Snapshot> testWapSnapshots = IcebergTestUtil.getWapSnapshots().subList(0, 1);
    // add 1 staged snapshot to the base metadata
    TableMetadata base =
        TableMetadata.buildFrom(BASE_TABLE_METADATA).addSnapshot(testWapSnapshots.get(0)).build();
    Map<String, String> properties = new HashMap<>(base.properties());
    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      // delete staged snapshots in the new metadata
      properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);

      TableMetadata metadata = base.replaceProperties(properties);
      openHouseInternalTableOperations.doCommit(base, metadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());
      Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();

      // verify nothing happens
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("staged_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("appended_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("cherry_picked_snapshots")));
      Assertions.assertEquals(
          null, updatedProperties.get(getCanonicalFieldName("deleted_snapshots")));
      Mockito.verify(mockHouseTableRepository, Mockito.times(1)).save(Mockito.eq(mockHouseTable));
    }
  }

  @Test
  void testRebuildPartitionSpecUnpartitioned() {
    Schema originalSchema =
        new Schema(Types.NestedField.optional(1, "field1", Types.StringType.get()));

    PartitionSpec originalSpec = PartitionSpec.unpartitioned();
    PartitionSpec rebuiltSpec =
        OpenHouseInternalTableOperations.rebuildPartitionSpec(
            originalSpec, originalSchema, originalSchema);

    Assertions.assertNotNull(rebuiltSpec);
    Assertions.assertTrue(rebuiltSpec.isUnpartitioned());
  }

  @Test
  void testRebuildPartitionSpec_NewSchemaSameFieldIds() {
    Schema originalSchema =
        new Schema(
            Types.NestedField.optional(1, "field1", Types.StringType.get()),
            Types.NestedField.optional(2, "field2", Types.IntegerType.get()),
            Types.NestedField.optional(3, "field3", Types.LongType.get()),
            Types.NestedField.optional(4, "field4", Types.LongType.get()));

    PartitionSpec originalSpec =
        PartitionSpec.builderFor(originalSchema)
            .identity("field1")
            .bucket("field2", 10)
            .truncate("field3", 20)
            .build();

    PartitionSpec rebuiltSpec =
        OpenHouseInternalTableOperations.rebuildPartitionSpec(
            originalSpec, originalSchema, originalSchema);

    Assertions.assertNotNull(rebuiltSpec);
    Assertions.assertEquals(0, rebuiltSpec.specId());
    Assertions.assertEquals(3, rebuiltSpec.fields().size());
    Assertions.assertEquals("field1", rebuiltSpec.fields().get(0).name());
    Assertions.assertEquals("identity", rebuiltSpec.fields().get(0).transform().toString());
    // field id in table schema should match sourceid in partition spec
    Assertions.assertEquals(1, rebuiltSpec.fields().get(0).sourceId());
    // Iceberg internally appends _bucket to partition field name
    Assertions.assertEquals("field2_bucket", rebuiltSpec.fields().get(1).name());
    Assertions.assertEquals("bucket[10]", rebuiltSpec.fields().get(1).transform().toString());
    Assertions.assertEquals(2, rebuiltSpec.fields().get(1).sourceId());
    // Iceberg internally appends _trunc to partition field name
    Assertions.assertEquals("field3_trunc", rebuiltSpec.fields().get(2).name());
    Assertions.assertEquals("truncate[20]", rebuiltSpec.fields().get(2).transform().toString());
    Assertions.assertEquals(3, rebuiltSpec.fields().get(2).sourceId());
  }

  @Test
  void testRebuildPartitionSpec_NewSchemaDifferentFieldIds() {
    Schema originalSchema =
        new Schema(
            Types.NestedField.optional(1, "field1", Types.StringType.get()),
            Types.NestedField.optional(2, "field2", Types.IntegerType.get()),
            Types.NestedField.optional(3, "field3", Types.LongType.get()),
            Types.NestedField.optional(4, "field4", Types.LongType.get()));

    PartitionSpec originalSpec =
        PartitionSpec.builderFor(originalSchema)
            .identity("field1")
            .bucket("field2", 10)
            .truncate("field3", 20)
            .build();

    // field2 and field3 have different fieldids compared to original schema
    Schema newSchema =
        new Schema(
            Types.NestedField.optional(1, "field1", Types.StringType.get()),
            Types.NestedField.optional(3, "field2", Types.IntegerType.get()),
            Types.NestedField.optional(2, "field3", Types.LongType.get()),
            Types.NestedField.optional(4, "field4", Types.LongType.get()));

    PartitionSpec rebuiltSpec =
        OpenHouseInternalTableOperations.rebuildPartitionSpec(
            originalSpec, originalSchema, newSchema);

    Assertions.assertNotNull(rebuiltSpec);
    Assertions.assertEquals(0, rebuiltSpec.specId());
    Assertions.assertEquals(3, rebuiltSpec.fields().size());
    Assertions.assertEquals("field1", rebuiltSpec.fields().get(0).name());
    Assertions.assertEquals("identity", rebuiltSpec.fields().get(0).transform().toString());
    // field id in table schema should match sourceid in partition spec
    Assertions.assertEquals(1, rebuiltSpec.fields().get(0).sourceId());
    // Iceberg internally appends _bucket to partition field name
    Assertions.assertEquals("field2_bucket", rebuiltSpec.fields().get(1).name());
    Assertions.assertEquals("bucket[10]", rebuiltSpec.fields().get(1).transform().toString());
    Assertions.assertEquals(3, rebuiltSpec.fields().get(1).sourceId());
    // Iceberg internally appends _trunc to partition field name
    Assertions.assertEquals("field3_trunc", rebuiltSpec.fields().get(2).name());
    Assertions.assertEquals("truncate[20]", rebuiltSpec.fields().get(2).transform().toString());
    Assertions.assertEquals(2, rebuiltSpec.fields().get(2).sourceId());
  }

  @Test
  void testRebuildPartitionSpec_fieldMissingInNewSchema() {
    Schema originalSchema =
        new Schema(Types.NestedField.optional(1, "field1", Types.StringType.get()));

    PartitionSpec originalSpec =
        PartitionSpec.builderFor(originalSchema).identity("field1").build();

    Schema newSchema = new Schema(Types.NestedField.optional(2, "field2", Types.IntegerType.get()));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                OpenHouseInternalTableOperations.rebuildPartitionSpec(
                    originalSpec, originalSchema, newSchema));

    Assertions.assertEquals(
        "Field field1 does not exist in the new schema", exception.getMessage());
  }

  @Test
  void testRebuildSortOrder_NewSchemaSameFieldIds() {
    Schema originalSchema =
        new Schema(
            Types.NestedField.optional(1, "field1", Types.StringType.get()),
            Types.NestedField.optional(2, "field2", Types.IntegerType.get()));

    SortOrder originalSortOrder =
        SortOrder.builderFor(originalSchema).asc("field1").desc("field2").build();

    Schema newSchema =
        new Schema(
            Types.NestedField.optional(1, "field1", Types.StringType.get()),
            Types.NestedField.optional(2, "field2", Types.IntegerType.get()));

    SortOrder rebuiltSortOrder =
        OpenHouseInternalTableOperations.rebuildSortOrder(originalSortOrder, newSchema);

    Assertions.assertNotNull(rebuiltSortOrder);
    Assertions.assertEquals(2, rebuiltSortOrder.fields().size());
    Assertions.assertEquals(SortDirection.ASC, rebuiltSortOrder.fields().get(0).direction());
    Assertions.assertEquals(1, rebuiltSortOrder.fields().get(0).sourceId());
    Assertions.assertEquals(SortDirection.DESC, rebuiltSortOrder.fields().get(1).direction());
    Assertions.assertEquals(2, rebuiltSortOrder.fields().get(1).sourceId());
  }

  @Test
  void testRebuildSortOrder_NewSchemaDifferentFieldIds() {
    Schema originalSchema =
        new Schema(
            Types.NestedField.optional(1, "field1", Types.StringType.get()),
            Types.NestedField.optional(2, "field2", Types.IntegerType.get()));

    SortOrder originalSortOrder =
        SortOrder.builderFor(originalSchema).asc("field1").desc("field2").build();

    Schema newSchema =
        new Schema(
            Types.NestedField.optional(2, "field1", Types.StringType.get()),
            Types.NestedField.optional(1, "field2", Types.IntegerType.get()));

    SortOrder rebuiltSortOrder =
        OpenHouseInternalTableOperations.rebuildSortOrder(originalSortOrder, newSchema);

    Assertions.assertNotNull(rebuiltSortOrder);
    Assertions.assertEquals(2, rebuiltSortOrder.fields().size());
    Assertions.assertEquals(SortDirection.ASC, rebuiltSortOrder.fields().get(0).direction());
    Assertions.assertEquals(2, rebuiltSortOrder.fields().get(0).sourceId());
    Assertions.assertEquals(SortDirection.DESC, rebuiltSortOrder.fields().get(1).direction());
    Assertions.assertEquals(1, rebuiltSortOrder.fields().get(1).sourceId());
  }

  @Test
  void testRebuildSortOrder_fieldMissingInNewSchema() {
    Schema originalSchema =
        new Schema(Types.NestedField.optional(1, "field1", Types.StringType.get()));

    SortOrder originalSortOrder = SortOrder.builderFor(originalSchema).asc("field1").build();

    Schema newSchema = new Schema(Types.NestedField.optional(2, "field2", Types.IntegerType.get()));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> OpenHouseInternalTableOperations.rebuildSortOrder(originalSortOrder, newSchema));

    Assertions.assertEquals(
        "Field field1 does not exist in the new schema", exception.getMessage());
  }

  @Test
  void testRefreshMetadataIncludesDatabaseAndTableTags() {
    // Create a real SimpleMeterRegistry to capture metrics
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricsReporter realMetricsReporter =
        new MetricsReporter(meterRegistry, "TEST_CATALOG", Lists.newArrayList());

    // Create instance with real metrics reporter
    OpenHouseInternalTableOperations operationsWithRealMetrics =
        new OpenHouseInternalTableOperations(
            mockHouseTableRepository,
            new HadoopFileIO(new Configuration()),
            Mockito.mock(SnapshotInspector.class),
            mockHouseTableMapper,
            TEST_TABLE_IDENTIFIER,
            realMetricsReporter);

    // Setup mock behavior
    HouseTablePrimaryKey primaryKey =
        HouseTablePrimaryKey.builder()
            .databaseId(TEST_TABLE_IDENTIFIER.namespace().toString())
            .tableId(TEST_TABLE_IDENTIFIER.name())
            .build();
    when(mockHouseTableRepository.findById(primaryKey)).thenReturn(Optional.of(mockHouseTable));
    when(mockHouseTable.getTableLocation()).thenReturn("test_metadata_location");

    // Call refreshMetadata directly to avoid the full doRefresh flow
    try {
      operationsWithRealMetrics.refreshMetadata("test_metadata_location");
    } catch (Exception e) {
      // We expect this to fail since it's not a real metadata file, but the timer should still be
      // recorded
    }

    // Verify that the timer was created with the correct metric name and tags
    String expectedMetricName =
        "TEST_CATALOG_" + InternalCatalogMetricsConstant.METADATA_RETRIEVAL_LATENCY;

    // Find the timer in the registry
    io.micrometer.core.instrument.Timer timer = meterRegistry.find(expectedMetricName).timer();
    Assertions.assertNotNull(timer, "Timer should be created");

    // Verify the tags are present
    boolean hasDatabaseTag =
        timer.getId().getTags().stream()
            .anyMatch(
                tag ->
                    tag.getKey().equals(InternalCatalogMetricsConstant.DATABASE_TAG)
                        && tag.getValue().equals("test_db"));
    boolean hasTableTag =
        timer.getId().getTags().stream()
            .anyMatch(
                tag ->
                    tag.getKey().equals(InternalCatalogMetricsConstant.TABLE_TAG)
                        && tag.getValue().equals("test_table"));

    Assertions.assertTrue(hasDatabaseTag, "Timer should have database tag with value 'test_db'");
    Assertions.assertTrue(hasTableTag, "Timer should have table tag with value 'test_table'");

    // Verify the timer was actually used (count > 0)
    Assertions.assertTrue(timer.count() > 0, "Timer should have been used at least once");
  }
}
