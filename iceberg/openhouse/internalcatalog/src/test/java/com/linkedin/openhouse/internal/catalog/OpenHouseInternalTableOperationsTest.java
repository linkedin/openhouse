package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.metrics.micrometer.MetricsReporter;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.local.LocalStorage;
import com.linkedin.openhouse.cluster.storage.local.LocalStorageClient;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapper;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.model.HouseTablePrimaryKey;
import com.linkedin.openhouse.internal.catalog.repository.HouseTableRepository;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableCallerException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableConcurrentUpdateException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.utils.MetadataUpdateUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefParser;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
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
  @Mock private FileSystem mockFileSystem;
  @Mock private LocalStorageClient mockLocalStorageClient;
  @Mock private FSDataInputStream mockFSDataInputStream;
  @Mock private FSDataOutputStream mockFSDataOutputStream;

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
            new MetricsReporter(new SimpleMeterRegistry(), "TEST_CATALOG", Lists.newArrayList()),
            fileIOManager);

    // Create a separate instance with mock metrics reporter for testing metrics
    openHouseInternalTableOperationsWithMockMetrics =
        new OpenHouseInternalTableOperations(
            mockHouseTableRepository,
            fileIO,
            Mockito.mock(SnapshotInspector.class),
            mockHouseTableMapper,
            TEST_TABLE_IDENTIFIER,
            mockMetricsReporter,
            fileIOManager);

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
  void testDoCommitUpdateMetadataForInitalVersionCommit() throws IOException {
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogConstants.LAST_UPDATED_MS, "1233232423");
    properties.put(CatalogConstants.OPENHOUSE_IS_TABLE_REPLICATED_KEY, "true");
    properties.put(CatalogConstants.OPENHOUSE_TABLE_VERSION, CatalogConstants.INITIAL_VERSION);
    TableMetadata base = BASE_TABLE_METADATA;

    TableMetadata metadata = base.replaceProperties(properties);

    // Setup mocks for filesystem operations
    LocalStorage mockLocalStorage = mock(LocalStorage.class);
    when(fileIOManager.getStorage(any(FileIO.class))).thenReturn(mockLocalStorage);
    when(mockLocalStorage.getClient()).thenReturn((StorageClient) mockLocalStorageClient);
    when(mockLocalStorageClient.getNativeClient()).thenReturn(mockFileSystem);

    // Mock filesystem operations for MetadataUpdateUtils.updateMetadataField
    when(mockFileSystem.open(any(Path.class))).thenReturn(mockFSDataInputStream);
    when(mockFileSystem.create(any(Path.class), eq(true))).thenReturn(mockFSDataOutputStream);

    // Mock input stream to return JSON content that can be parsed
    String mockJsonContent = "{\"last-updated-ms\": 1233232422}";
    when(mockFSDataInputStream.read(any(byte[].class)))
        .thenAnswer(
            invocation -> {
              byte[] buffer = invocation.getArgument(0);
              byte[] content = mockJsonContent.getBytes();
              System.arraycopy(content, 0, buffer, 0, Math.min(content.length, buffer.length));
              return content.length;
            });
    when(mockFSDataInputStream.read()).thenReturn(-1); // EOF

    try (MockedStatic<MetadataUpdateUtils> mockedMetadataUpdateUtils =
        mockStatic(MetadataUpdateUtils.class)) {
      openHouseInternalTableOperations.doCommit(base, metadata);

      // Verify updateMetadataField was called
      mockedMetadataUpdateUtils.verify(
          () ->
              MetadataUpdateUtils.updateMetadataField(
                  eq(mockFileSystem), anyString(), eq("last-updated-ms"), eq(1233232423L)));
    }

    Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());

    Map<String, String> updatedProperties = tblMetadataCaptor.getValue().properties();
    Assertions.assertEquals(
        CatalogConstants.INITIAL_VERSION,
        updatedProperties.get(getCanonicalFieldName("tableVersion")));

    Assertions.assertTrue(updatedProperties.containsKey(getCanonicalFieldName("tableLocation")));
    Mockito.verify(mockHouseTableRepository, Mockito.times(1)).save(Mockito.eq(mockHouseTable));

    // Verify filesystem operations were performed
    verify(fileIOManager).getStorage(any(FileIO.class));
    // Called 3 times: 2x for instanceof checks, 1x for assignment
    verify(mockLocalStorage, times(3)).getClient();
    verify(mockLocalStorageClient).getNativeClient();
  }

  @Test
  void testDoCommitUpdateMetadataNotCalledForNonReplicatedTable() throws IOException {
    Map<String, String> properties = new HashMap<>();
    properties.put("last-updated-ms", "1233232423");
    properties.put(CatalogConstants.OPENHOUSE_TABLE_VERSION, CatalogConstants.INITIAL_VERSION);
    TableMetadata base = BASE_TABLE_METADATA;

    TableMetadata metadata = base.replaceProperties(properties);

    // Setup mocks for filesystem operations
    LocalStorage mockLocalStorage = mock(LocalStorage.class);
    when(fileIOManager.getStorage(any(FileIO.class))).thenReturn(mockLocalStorage);
    when(mockLocalStorage.getClient()).thenReturn((StorageClient) mockLocalStorageClient);
    when(mockLocalStorageClient.getNativeClient()).thenReturn(mockFileSystem);

    try (MockedStatic<MetadataUpdateUtils> mockedMetadataUpdateUtils =
        mockStatic(MetadataUpdateUtils.class)) {
      openHouseInternalTableOperations.doCommit(base, metadata);

      // Verify updateMetadataField was NOT called since table is not replicated
      mockedMetadataUpdateUtils.verifyNoInteractions();
    }

    Mockito.verify(mockHouseTableRepository, Mockito.times(1)).save(Mockito.any(HouseTable.class));
  }

  @Test
  void testDoCommitUpdateMetadataNotCalledForNonInitialVersionCommit() throws IOException {
    Map<String, String> properties = new HashMap<>();
    properties.put("last-updated-ms", "1233232423");
    properties.put(CatalogConstants.OPENHOUSE_IS_TABLE_REPLICATED_KEY, "true");
    properties.put(CatalogConstants.OPENHOUSE_TABLE_VERSION, "v1.0.0");

    // Set tableLocation to a non-INITIAL_VERSION value so that tableVersion gets set to this value
    // This will cause isReplicatedTableCreate to return false since tableVersion != INITIAL_VERSION
    properties.put(getCanonicalFieldName("tableLocation"), "some-existing-table-location");

    // Use existing table metadata (base != null) to simulate a snapshot commit rather than table
    // creation
    TableMetadata base = BASE_TABLE_METADATA;
    TableMetadata metadata = base.replaceProperties(properties);

    // Setup mocks for filesystem operations
    LocalStorage mockLocalStorage = mock(LocalStorage.class);
    when(fileIOManager.getStorage(any(FileIO.class))).thenReturn(mockLocalStorage);
    when(mockLocalStorage.getClient()).thenReturn((StorageClient) mockLocalStorageClient);
    when(mockLocalStorageClient.getNativeClient()).thenReturn(mockFileSystem);

    try (MockedStatic<MetadataUpdateUtils> mockedMetadataUpdateUtils =
        mockStatic(MetadataUpdateUtils.class)) {
      openHouseInternalTableOperations.doCommit(base, metadata);

      // Verify updateMetadataField was NOT called since this is not an initial version commit
      mockedMetadataUpdateUtils.verifyNoInteractions();
    }

    Mockito.verify(mockHouseTableRepository, Mockito.times(1)).save(Mockito.any(HouseTable.class));
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
  void testDoCommitWithValidSnapshotDeletion() throws IOException {
    TableMetadata metadata =
        BASE_TABLE_METADATA.replaceProperties(ImmutableMap.of("random", "value"));
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    Map<String, String> properties = new HashMap<>(metadata.properties());

    // The key insight: SNAPSHOTS_JSON_KEY determines what snapshots SHOULD exist after commit
    // Only include snapshot 2 - this means snapshots 0 and 1 should be deleted
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY,
        SnapshotsUtil.serializedSnapshots(testSnapshots.subList(2, 3))); // Only snapshot 2
    properties.put(
        CatalogConstants.SNAPSHOTS_REFS_KEY,
        SnapshotsUtil.serializeMap(
            IcebergTestUtil.obtainSnapshotRefsFromSnapshot(
                testSnapshots.get(2)))); // snapshot 2 -> main
    properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);
    metadata = metadata.replaceProperties(properties);

    // Create initial metadata with snapshots 0, 1, 2 where only snapshot 2 is referenced
    TableMetadata metadataWithSnapshots =
        TableMetadata.buildFrom(metadata)
            .addSnapshot(testSnapshots.get(0)) // Unreferenced - will be deleted
            .addSnapshot(testSnapshots.get(1)) // Unreferenced - will be deleted
            .setBranchSnapshot(
                testSnapshots.get(2), SnapshotRef.MAIN_BRANCH) // Referenced - will be kept
            .build();

    // Target metadata: same branch setup but snapshots 0,1 removed via SNAPSHOTS_JSON_KEY
    TableMetadata metadataWithSnapshotsDeleted =
        TableMetadata.buildFrom(metadata)
            .setBranchSnapshot(
                testSnapshots.get(2), SnapshotRef.MAIN_BRANCH) // Only snapshot 2 remains
            .build();

    // This should succeed because snapshots 0 and 1 are unreferenced and can be safely deleted
    Assertions.assertDoesNotThrow(
        () ->
            openHouseInternalTableOperations.doCommit(
                metadataWithSnapshots, metadataWithSnapshotsDeleted));

    // ideally we also verify that snapshots 0 and 1 are deleted, but doCommit doesn't return the
    // metadata with the deleted snapshots
  }

  @Test
  void testDoCommitSnapshotsValidationThrowsException() throws IOException {
    TableMetadata metadata =
        BASE_TABLE_METADATA.replaceProperties(ImmutableMap.of("random", "value"));
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    Map<String, String> properties = new HashMap<>(metadata.properties());

    // The key issue: SNAPSHOTS_JSON_KEY says to keep only snapshot 2, but snapshot 1 is referenced
    // by main
    // This creates a conflict - we're trying to delete snapshot 1 but it's still referenced
    properties.put(
        CatalogConstants.SNAPSHOTS_JSON_KEY,
        SnapshotsUtil.serializedSnapshots(
            testSnapshots.subList(2, 3))); // Only snapshot 2 should remain
    properties.put(
        CatalogConstants.SNAPSHOTS_REFS_KEY,
        SnapshotsUtil.serializeMap(
            IcebergTestUtil.obtainSnapshotRefsFromSnapshot(
                testSnapshots.get(1)))); // But main refs snapshot 1
    properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);
    metadata = metadata.replaceProperties(properties);

    // Create initial metadata with snapshots 1 and 2, where snapshot 1 is referenced by main
    TableMetadata metadataWithSnapshots =
        TableMetadata.buildFrom(metadata)
            .setBranchSnapshot(testSnapshots.get(1), SnapshotRef.MAIN_BRANCH) // snapshot 1 -> main
            .addSnapshot(testSnapshots.get(2)) // snapshot 2 exists but unreferenced initially
            .build();

    // Target metadata tries to delete snapshot 1 (not in SNAPSHOTS_JSON_KEY) but main still refs it
    TableMetadata metadataWithSnapshotsDeleted =
        TableMetadata.buildFrom(metadata)
            .setBranchSnapshot(
                testSnapshots.get(1), SnapshotRef.MAIN_BRANCH) // main still points to snapshot 1
            .build();

    // This should throw exception because snapshot 1 is marked for deletion but still referenced by
    // main
    Assertions.assertThrows(
        CommitStateUnknownException.class,
        () ->
            openHouseInternalTableOperations.doCommit(
                metadataWithSnapshots, metadataWithSnapshotsDeleted),
        "Should throw exception when trying to delete referenced snapshots");
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
            openHouseInternalTableOperations.applySnapshotOperations(
                metadata, snapshots, ImmutableMap.of(), false));
    // the latest snapshots have larger timestamp than the previous metadata timestamp, so it should
    // pass the validation
    snapshots.addAll(IcebergTestUtil.getFutureSnapshots());
    openHouseInternalTableOperations.applySnapshotOperations(
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
  void testRefreshMetadataIncludesDatabaseTag() {
    testMetricIncludesDatabaseTag(
        InternalCatalogMetricsConstant.METADATA_RETRIEVAL_LATENCY,
        this::setupRefreshMetadataTest,
        this::executeRefreshMetadata,
        "Timer should not have table tag (removed because the table tag has super high cardinality and overloads metric emission max size)");
  }

  @Test
  void testCommitMetadataUpdateIncludesDatabaseTag() {
    testMetricIncludesDatabaseTag(
        InternalCatalogMetricsConstant.METADATA_UPDATE_LATENCY,
        this::setupCommitMetadataTest,
        this::executeCommitMetadata,
        "Timer should not have table tag (only database dimension should be included)");
  }

  @Test
  void testRefreshMetadataLatencyHasHistogramBuckets() {
    testMetricHasHistogramBuckets(
        InternalCatalogMetricsConstant.METADATA_RETRIEVAL_LATENCY,
        this::setupRefreshMetadataTest,
        this::executeRefreshMetadata);
  }

  @Test
  void testCommitMetadataUpdateLatencyHasHistogramBuckets() {
    testMetricHasHistogramBuckets(
        InternalCatalogMetricsConstant.METADATA_UPDATE_LATENCY,
        this::setupCommitMetadataTest,
        this::executeCommitMetadata);
  }

  /**
   * Common test method for verifying metrics include database tag but not table tag.
   *
   * @param expectedMetricSuffix The metric name suffix (without catalog prefix)
   * @param setupFunction Function to set up test-specific mocks
   * @param executeFunction Function to execute the operation that should record metrics
   * @param noTableTagMessage Custom message for table tag assertion
   */
  private void testMetricIncludesDatabaseTag(
      String expectedMetricSuffix,
      Consumer<OpenHouseInternalTableOperations> setupFunction,
      Consumer<OpenHouseInternalTableOperations> executeFunction,
      String noTableTagMessage) {

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
            realMetricsReporter,
            fileIOManager);

    // Setup test-specific mocks
    setupFunction.accept(operationsWithRealMetrics);

    // Execute the operation that should record the metric
    executeFunction.accept(operationsWithRealMetrics);

    // Verify the metric was recorded with correct tags
    verifyMetricTags(meterRegistry, expectedMetricSuffix, noTableTagMessage);
  }

  /**
   * Common test method for verifying that Timer metrics have histogram buckets configured.
   *
   * @param expectedMetricSuffix The metric name suffix (without catalog prefix)
   * @param setupFunction Function to set up test-specific mocks
   * @param executeFunction Function to execute the operation that should record metrics
   */
  private void testMetricHasHistogramBuckets(
      String expectedMetricSuffix,
      Consumer<OpenHouseInternalTableOperations> setupFunction,
      Consumer<OpenHouseInternalTableOperations> executeFunction) {

    // Create a real SimpleMeterRegistry with histogram configuration
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    // Configure the registry to enable histogram buckets for all timers
    // This mimics the application.properties setting:
    // management.metrics.distribution.percentiles-histogram.all=true
    meterRegistry
        .config()
        .meterFilter(
            new io.micrometer.core.instrument.config.MeterFilter() {
              @Override
              public io.micrometer.core.instrument.distribution.DistributionStatisticConfig
                  configure(
                      io.micrometer.core.instrument.Meter.Id id,
                      io.micrometer.core.instrument.distribution.DistributionStatisticConfig
                          config) {
                if (id.getType() == io.micrometer.core.instrument.Meter.Type.TIMER) {
                  return io.micrometer.core.instrument.distribution.DistributionStatisticConfig
                      .builder()
                      .percentilesHistogram(true)
                      .build()
                      .merge(config);
                }
                return config;
              }
            });

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
            realMetricsReporter,
            fileIOManager);

    // Setup test-specific mocks
    setupFunction.accept(operationsWithRealMetrics);

    // Execute the operation that should record the metric
    executeFunction.accept(operationsWithRealMetrics);

    // Verify the metric has histogram buckets
    verifyMetricHistogramBuckets(meterRegistry, expectedMetricSuffix);
  }

  /** Sets up mocks specific to refresh metadata tests. */
  private void setupRefreshMetadataTest(OpenHouseInternalTableOperations operations) {
    HouseTablePrimaryKey primaryKey =
        HouseTablePrimaryKey.builder()
            .databaseId(TEST_TABLE_IDENTIFIER.namespace().toString())
            .tableId(TEST_TABLE_IDENTIFIER.name())
            .build();
    when(mockHouseTableRepository.findById(primaryKey)).thenReturn(Optional.of(mockHouseTable));
    when(mockHouseTable.getTableLocation()).thenReturn("test_metadata_location");
  }

  /** Sets up mocks specific to commit metadata tests. */
  private void setupCommitMetadataTest(OpenHouseInternalTableOperations operations) {
    when(mockHouseTableMapper.toHouseTable(Mockito.any(TableMetadata.class), Mockito.any()))
        .thenReturn(mockHouseTable);
    when(mockHouseTableRepository.save(Mockito.any())).thenReturn(mockHouseTable);
  }

  /** Executes refresh metadata operation for testing. */
  private void executeRefreshMetadata(OpenHouseInternalTableOperations operations) {
    try {
      operations.refreshMetadata("test_metadata_location");
    } catch (Exception e) {
      // We expect this to fail since it's not a real metadata file, but the timer should still be
      // recorded
    }
  }

  /** Executes commit metadata operation for testing. */
  private void executeCommitMetadata(OpenHouseInternalTableOperations operations) {
    // Create simple metadata for commit
    Map<String, String> properties = new HashMap<>(BASE_TABLE_METADATA.properties());
    properties.put(getCanonicalFieldName("tableLocation"), TEST_LOCATION);
    TableMetadata metadata = BASE_TABLE_METADATA.replaceProperties(properties);

    // Mock TableMetadataParser to avoid actual file writing but still trigger the metric recording
    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      try {
        operations.doCommit(BASE_TABLE_METADATA, metadata);
      } catch (Exception e) {
        // We expect this might fail due to mocked components, but the timer should still be
        // recorded
      }
    }
  }

  /**
   * Verifies that a metric was recorded with the correct tags (database tag present, table tag
   * absent).
   *
   * @param meterRegistry The meter registry to search for metrics
   * @param expectedMetricSuffix The expected metric name suffix
   * @param noTableTagMessage Custom message for table tag assertion
   */
  private void verifyMetricTags(
      SimpleMeterRegistry meterRegistry, String expectedMetricSuffix, String noTableTagMessage) {
    String expectedMetricName = "TEST_CATALOG_" + expectedMetricSuffix;

    // Find the timer in the registry
    io.micrometer.core.instrument.Timer timer = meterRegistry.find(expectedMetricName).timer();
    Assertions.assertNotNull(timer, "Timer should be created");

    // Verify the database tag is present
    boolean hasDatabaseTag =
        timer.getId().getTags().stream()
            .anyMatch(
                tag ->
                    tag.getKey().equals(InternalCatalogMetricsConstant.DATABASE_TAG)
                        && tag.getValue().equals("test_db"));

    // Verify the table tag is NOT present
    boolean hasTableTag =
        timer.getId().getTags().stream()
            .anyMatch(tag -> tag.getKey().equals(InternalCatalogMetricsConstant.TABLE_TAG));

    Assertions.assertTrue(hasDatabaseTag, "Timer should have database tag with value 'test_db'");
    Assertions.assertFalse(hasTableTag, noTableTagMessage);

    // Verify the timer was actually used (count > 0)
    Assertions.assertTrue(timer.count() > 0, "Timer should have been used at least once");
  }

  /**
   * Verifies that a Timer metric has histogram buckets configured.
   *
   * @param meterRegistry The meter registry to search for metrics
   * @param expectedMetricSuffix The expected metric name suffix
   */
  private void verifyMetricHistogramBuckets(
      SimpleMeterRegistry meterRegistry, String expectedMetricSuffix) {
    String expectedMetricName = "TEST_CATALOG_" + expectedMetricSuffix;

    // Find the timer in the registry
    io.micrometer.core.instrument.Timer timer = meterRegistry.find(expectedMetricName).timer();
    Assertions.assertNotNull(timer, "Timer should be created");

    // Verify the timer was actually used (count > 0)
    Assertions.assertTrue(timer.count() > 0, "Timer should have been used at least once");

    // Get the timer's snapshot to access histogram data
    io.micrometer.core.instrument.distribution.HistogramSnapshot snapshot = timer.takeSnapshot();

    // Verify histogram buckets are present
    io.micrometer.core.instrument.distribution.CountAtBucket[] buckets = snapshot.histogramCounts();
    Assertions.assertNotNull(buckets, "Timer should have histogram buckets");

    // Verify that basic histogram statistics are available
    // Check that total time and max are not null (and not NaN)
    double totalTime = timer.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS);
    double maxTime = timer.max(java.util.concurrent.TimeUnit.NANOSECONDS);

    // In Micrometer, totalTime and max return primitive doubles, never null, but may be 0 or NaN.
    // So, assertNotNull is not meaningful for primitives; instead, check for NaN.
    Assertions.assertFalse(Double.isNaN(totalTime), "Timer total time should not be NaN");
    Assertions.assertFalse(Double.isNaN(maxTime), "Timer max time should not be NaN");
  }

  // ===== SNAPSHOT DELETION SAFETY TESTS =====

  @Test
  void testDeleteSnapshotWithMainReference() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    // Create base metadata with multiple snapshots
    TableMetadata baseMetadata =
        TableMetadata.buildFrom(BASE_TABLE_METADATA)
            .addSnapshot(testSnapshots.get(0)) // Unreferenced - can be deleted
            .addSnapshot(testSnapshots.get(1)) // Unreferenced - can be deleted
            .addSnapshot(testSnapshots.get(2)) // Unreferenced - can be deleted
            .setBranchSnapshot(
                testSnapshots.get(3), SnapshotRef.MAIN_BRANCH) // Referenced - cannot be deleted
            .build();

    // Get the current head snapshot that is referenced by main branch
    Snapshot referencedSnapshot = testSnapshots.get(testSnapshots.size() - 1);

    // Attempt to delete a snapshot that is currently referenced by a branch
    List<Snapshot> snapshotsToDelete = List.of(referencedSnapshot);

    // Capture final variables for lambda
    final TableMetadata finalBase = baseMetadata;
    final List<Snapshot> finalSnapshotsToDelete = snapshotsToDelete;

    // This MUST throw IllegalArgumentException for referenced snapshots
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                openHouseInternalTableOperations.maybeDeleteSnapshots(
                    finalBase, finalSnapshotsToDelete),
            "Should throw IllegalArgumentException when trying to delete referenced snapshot");

    // Verify error message mentions the reference
    String expectedMessage =
        "Cannot expire " + referencedSnapshot.snapshotId() + ". Still referenced by refs:";
    Assertions.assertTrue(
        exception.getMessage().contains(expectedMessage)
            || exception.getMessage().contains("Still referenced by")
            || exception.getMessage().contains("referenced"),
        "Error message should indicate snapshot is still referenced: " + exception.getMessage());
  }

  @Test
  void testDeleteSnapshotWithNoReference() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    // Create base metadata with multiple snapshots
    TableMetadata base =
        TableMetadata.buildFrom(BASE_TABLE_METADATA)
            .addSnapshot(testSnapshots.get(0)) // Unreferenced - can be deleted
            .addSnapshot(testSnapshots.get(1)) // Unreferenced - can be deleted
            .addSnapshot(testSnapshots.get(2)) // Unreferenced - can be deleted
            .setBranchSnapshot(
                testSnapshots.get(3), SnapshotRef.MAIN_BRANCH) // Referenced - cannot be deleted
            .build();

    // Delete unreferenced snapshots (first two snapshots)
    List<Snapshot> unreferencedSnapshots = testSnapshots.subList(0, 2);

    TableMetadata result =
        openHouseInternalTableOperations.maybeDeleteSnapshots(base, unreferencedSnapshots);

    // Verify unreferenced snapshots were removed
    for (Snapshot unreferenced : unreferencedSnapshots) {
      boolean snapshotExists =
          result.snapshots().stream().anyMatch(s -> s.snapshotId() == unreferenced.snapshotId());
      Assertions.assertFalse(
          snapshotExists,
          "Unreferenced snapshot " + unreferenced.snapshotId() + " should be deleted");
    }

    // Verify referenced snapshot still exists
    Snapshot referencedSnapshot = testSnapshots.get(3);
    boolean referencedExists =
        result.snapshots().stream()
            .anyMatch(s -> s.snapshotId() == referencedSnapshot.snapshotId());
    Assertions.assertTrue(referencedExists, "Referenced snapshot should still exist");

    // Verify deletion tracking
    Map<String, String> properties = result.properties();
    String deletedSnapshots =
        properties.get(getCanonicalFieldName(CatalogConstants.DELETED_SNAPSHOTS));
    Assertions.assertNotNull(deletedSnapshots);

    for (Snapshot unreferenced : unreferencedSnapshots) {
      Assertions.assertTrue(
          deletedSnapshots.contains(Long.toString(unreferenced.snapshotId())),
          "Unreferenced snapshot should be tracked as deleted");
    }
  }

  @Test
  void testDeleteSnapshotWithMultipleReference() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    // Create metadata with snapshot referenced by multiple branches
    // Reference the same snapshot from multiple branches
    Snapshot sharedSnapshot = testSnapshots.get(1);
    TableMetadata baseMetadata =
        TableMetadata.buildFrom(BASE_TABLE_METADATA)
            .addSnapshot(sharedSnapshot) // Add snapshot first
            .setRef(
                SnapshotRef.MAIN_BRANCH,
                SnapshotRef.branchBuilder(sharedSnapshot.snapshotId()).build())
            .setRef(
                "feature_branch", SnapshotRef.branchBuilder(sharedSnapshot.snapshotId()).build())
            .build();
    // Add other snapshots to the metadata (skip index 1 - shared snapshot already added)
    List<Snapshot> snapshotsToAdd =
        IntStream.range(0, testSnapshots.size())
            .filter(i -> i != 1)
            .mapToObj(testSnapshots::get)
            .collect(Collectors.toList());

    for (Snapshot snapshot : snapshotsToAdd) {
      baseMetadata = TableMetadata.buildFrom(baseMetadata).addSnapshot(snapshot).build();
    }

    // Attempt to delete the shared snapshot
    List<Snapshot> snapshotsToDelete = List.of(sharedSnapshot);

    // Capture final variables for lambda
    final TableMetadata finalBase = baseMetadata;
    final List<Snapshot> finalSnapshotsToDelete = snapshotsToDelete;

    // This MUST throw IllegalArgumentException for snapshots referenced by multiple branches
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                openHouseInternalTableOperations.maybeDeleteSnapshots(
                    finalBase, finalSnapshotsToDelete),
            "Should throw IllegalArgumentException when trying to delete snapshot referenced by multiple branches");

    // Verify error message mentions multiple references
    String exceptionMessage = exception.getMessage();
    Assertions.assertTrue(
        exceptionMessage.contains("Still referenced by refs"),
        "Error message should indicate snapshot is still referenced by branches: "
            + exceptionMessage);
  }

  @Test
  void testDeleteSnapshotWithBranchReference() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    // Create base metadata with snapshots - add the tagged snapshot first
    Snapshot taggedSnapshot = testSnapshots.get(0);
    TableMetadata baseMetadata =
        TableMetadata.buildFrom(BASE_TABLE_METADATA)
            .addSnapshot(taggedSnapshot) // Add the snapshot first so it exists
            .setBranchSnapshot(testSnapshots.get(testSnapshots.size() - 1), SnapshotRef.MAIN_BRANCH)
            .setRef(
                "feature_branch",
                SnapshotRef.tagBuilder(taggedSnapshot.snapshotId()).build()) // Now create the tag
            .build();
    // Add remaining snapshots
    for (int i = 1; i < testSnapshots.size() - 1; i++) {
      baseMetadata =
          TableMetadata.buildFrom(baseMetadata).addSnapshot(testSnapshots.get(i)).build();
    }

    // Attempt to delete snapshot that has a tag reference
    List<Snapshot> snapshotsToDelete = List.of(taggedSnapshot);

    // Capture final variables for lambda
    final TableMetadata finalBase = baseMetadata;
    final List<Snapshot> finalSnapshotsToDelete = snapshotsToDelete;

    // This MUST throw IllegalArgumentException for snapshots referenced by tags
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                openHouseInternalTableOperations.maybeDeleteSnapshots(
                    finalBase, finalSnapshotsToDelete),
            "Should throw IllegalArgumentException when trying to delete snapshot referenced by tag");

    // Verify error message mentions tag reference
    String exceptionMessage = exception.getMessage();
    Assertions.assertTrue(
        exceptionMessage.contains("Still referenced by refs"),
        "Error message should indicate snapshot is still referenced by branches: "
            + exceptionMessage);
  }

  @Test
  void testDeleteEmptySnapshotList() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    // Create base metadata
    TableMetadata base = BASE_TABLE_METADATA;
    for (Snapshot snapshot : testSnapshots) {
      base =
          TableMetadata.buildFrom(base)
              .setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH)
              .build();
    }

    // Delete empty list
    List<Snapshot> emptyList = List.of();

    TableMetadata result = openHouseInternalTableOperations.maybeDeleteSnapshots(base, emptyList);

    // Verify no changes were made
    Assertions.assertEquals(
        base.snapshots().size(),
        result.snapshots().size(),
        "No snapshots should be deleted when list is empty");

    // Verify no deletion tracking properties were added
    Map<String, String> properties = result.properties();
    String deletedSnapshots =
        properties.get(getCanonicalFieldName(CatalogConstants.DELETED_SNAPSHOTS));
    Assertions.assertNull(deletedSnapshots, "No deleted snapshots property should be set");
  }

  @Test
  void testDeleteNullSnapshotList() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    // Create base metadata
    TableMetadata base = BASE_TABLE_METADATA;
    for (Snapshot snapshot : testSnapshots) {
      base =
          TableMetadata.buildFrom(base)
              .setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH)
              .build();
    }

    // Delete null list
    TableMetadata result = openHouseInternalTableOperations.maybeDeleteSnapshots(base, null);

    // Verify no changes were made
    Assertions.assertEquals(
        base.snapshots().size(),
        result.snapshots().size(),
        "No snapshots should be deleted when list is null");

    // Verify no deletion tracking properties were added
    Map<String, String> properties = result.properties();
    String deletedSnapshots =
        properties.get(getCanonicalFieldName(CatalogConstants.DELETED_SNAPSHOTS));
    Assertions.assertNull(deletedSnapshots, "No deleted snapshots property should be set");
  }

  @Test
  void testDeleteNonExistentSnapshot() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    // Create base metadata
    TableMetadata base = BASE_TABLE_METADATA;
    for (Snapshot snapshot : testSnapshots) {
      base =
          TableMetadata.buildFrom(base)
              .setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH)
              .build();
    }

    // Create a snapshot that doesn't exist in the metadata
    List<Snapshot> extraSnapshots = IcebergTestUtil.getExtraSnapshots();
    Snapshot nonExistentSnapshot = extraSnapshots.get(0);

    List<Snapshot> snapshotsToDelete = List.of(nonExistentSnapshot);

    TableMetadata result =
        openHouseInternalTableOperations.maybeDeleteSnapshots(base, snapshotsToDelete);

    // Verify original snapshots are unchanged
    Assertions.assertEquals(
        base.snapshots().size(),
        result.snapshots().size(),
        "Snapshot count should be unchanged when deleting non-existent snapshot");

    // Verify deletion is still tracked (documenting current behavior)
    Map<String, String> properties = result.properties();
    String deletedSnapshots =
        properties.get(getCanonicalFieldName(CatalogConstants.DELETED_SNAPSHOTS));
    Assertions.assertNotNull(deletedSnapshots);
    Assertions.assertTrue(
        deletedSnapshots.contains(Long.toString(nonExistentSnapshot.snapshotId())),
        "Non-existent snapshot should still be tracked as deleted");
  }

  @Test
  void testDeleteSnapshotMetricsRecorded() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    // Create base metadata
    TableMetadata base = BASE_TABLE_METADATA;
    for (Snapshot snapshot : testSnapshots) {
      base = TableMetadata.buildFrom(base).addSnapshot(snapshot).build();
    }

    // Delete some snapshots
    List<Snapshot> snapshotsToDelete = testSnapshots.subList(0, 2);

    // Use the operations instance with mock metrics reporter
    openHouseInternalTableOperationsWithMockMetrics.maybeDeleteSnapshots(base, snapshotsToDelete);

    // Verify metrics were recorded
    Mockito.verify(mockMetricsReporter)
        .count(
            eq(InternalCatalogMetricsConstant.SNAPSHOTS_DELETED_CTR),
            eq((double) snapshotsToDelete.size()));
  }

  @Test
  void testDeleteSnapshotMetricsRecordedBranch() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    // Create base metadata with snapshots that have branch references
    TableMetadata base =
        TableMetadata.buildFrom(BASE_TABLE_METADATA)
            .addSnapshot(testSnapshots.get(0)) // Unreferenced - can be deleted
            .addSnapshot(testSnapshots.get(1)) // Unreferenced - can be deleted
            .setBranchSnapshot(
                testSnapshots.get(2), SnapshotRef.MAIN_BRANCH) // Referenced - cannot be deleted
            .build();

    // Delete unreferenced snapshots (emits metrics for basic deletion)
    List<Snapshot> snapshotsToDelete = testSnapshots.subList(0, 2);

    // Use the operations instance with mock metrics reporter
    openHouseInternalTableOperationsWithMockMetrics.maybeDeleteSnapshots(base, snapshotsToDelete);

    // Verify metrics were recorded for the basic deletion
    Mockito.verify(mockMetricsReporter)
        .count(
            eq(InternalCatalogMetricsConstant.SNAPSHOTS_DELETED_CTR),
            eq((double) snapshotsToDelete.size()));
  }

  @Test
  void testDeleteSnapshotMetricsRecordedNonExistent() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    // Create base metadata
    TableMetadata base = BASE_TABLE_METADATA;
    for (Snapshot snapshot : testSnapshots) {
      base =
          TableMetadata.buildFrom(base)
              .setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH)
              .build();
    }

    // Create a snapshot that doesn't exist in the metadata
    List<Snapshot> extraSnapshots = IcebergTestUtil.getExtraSnapshots();
    Snapshot nonExistentSnapshot = extraSnapshots.get(0);
    List<Snapshot> snapshotsToDelete = List.of(nonExistentSnapshot);

    // Use the operations instance with mock metrics reporter
    openHouseInternalTableOperationsWithMockMetrics.maybeDeleteSnapshots(base, snapshotsToDelete);

    // Verify metrics are still recorded even for non-existent snapshots
    Mockito.verify(mockMetricsReporter)
        .count(
            eq(InternalCatalogMetricsConstant.SNAPSHOTS_DELETED_CTR),
            eq((double) snapshotsToDelete.size()));
  }

  @Test
  void testDeleteAllSnapshotsFailsWhenMainBranchReferenced() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    // Create base metadata with all snapshots, where the last one is referenced by main branch
    TableMetadata tempBase =
        testSnapshots.subList(0, testSnapshots.size() - 1).stream()
            .reduce(
                BASE_TABLE_METADATA,
                (metadata, snapshot) ->
                    TableMetadata.buildFrom(metadata).addSnapshot(snapshot).build(),
                (m1, m2) -> m2);
    final TableMetadata base =
        TableMetadata.buildFrom(tempBase)
            .setBranchSnapshot(testSnapshots.get(testSnapshots.size() - 1), SnapshotRef.MAIN_BRANCH)
            .build();

    // Attempt to delete ALL snapshots (including the one referenced by main)
    List<Snapshot> allSnapshots = new ArrayList<>(testSnapshots);

    // This should fail because we cannot delete the snapshot referenced by main branch
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> openHouseInternalTableOperations.maybeDeleteSnapshots(base, allSnapshots),
            "Should throw IllegalArgumentException when trying to delete all snapshots including main branch reference");

    // Verify error message indicates the snapshot is still referenced
    String exceptionMessage = exception.getMessage();
    Assertions.assertTrue(
        exceptionMessage.contains("Still referenced by refs")
            || exceptionMessage.contains("referenced")
            || exceptionMessage.contains("Cannot expire"),
        "Error message should indicate snapshot is still referenced: " + exceptionMessage);
  }

  @Test
  void testDeleteAllUnreferencedSnapshotsSucceeds() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    // Create base metadata with unreferenced snapshots only (no main branch or other refs)
    TableMetadata tempBase = BASE_TABLE_METADATA;
    for (Snapshot snapshot : testSnapshots) {
      tempBase = TableMetadata.buildFrom(tempBase).addSnapshot(snapshot).build();
    }
    final TableMetadata base = tempBase;
    // Note: No setBranchSnapshot or setRef calls - all snapshots are unreferenced

    // Attempt to delete all unreferenced snapshots
    List<Snapshot> allSnapshots = new ArrayList<>(testSnapshots);

    // This should succeed since no snapshots are referenced by any branch/tag
    TableMetadata result =
        Assertions.assertDoesNotThrow(
            () -> openHouseInternalTableOperations.maybeDeleteSnapshots(base, allSnapshots),
            "Should succeed when deleting all unreferenced snapshots");

    // Verify all snapshots were removed from the metadata
    Assertions.assertEquals(
        0,
        result.snapshots().size(),
        "All unreferenced snapshots should be deleted, resulting in empty snapshots list");

    // Verify deletion tracking shows all snapshots were deleted
    Map<String, String> properties = result.properties();
    String deletedSnapshots =
        properties.get(getCanonicalFieldName(CatalogConstants.DELETED_SNAPSHOTS));
    Assertions.assertNotNull(deletedSnapshots, "Deleted snapshots should be tracked");

    for (Snapshot snapshot : allSnapshots) {
      Assertions.assertTrue(
          deletedSnapshots.contains(Long.toString(snapshot.snapshotId())),
          "Snapshot " + snapshot.snapshotId() + " should be tracked as deleted");
    }
  }

  @Test
  void testValidMultipleBranchesWithDifferentSnapshots() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    // Create base metadata
    TableMetadata base =
        TableMetadata.buildFrom(BASE_TABLE_METADATA)
            .setBranchSnapshot(testSnapshots.get(0), SnapshotRef.MAIN_BRANCH)
            .build();

    // Add multiple new snapshots
    List<Snapshot> newSnapshots = testSnapshots.subList(1, 4); // snapshots 1, 2, 3

    // Create snapshotRefs where each branch points to a DIFFERENT snapshot (valid scenario)
    Map<String, SnapshotRef> validRefs = new HashMap<>();
    validRefs.put("branch_a", SnapshotRef.branchBuilder(testSnapshots.get(1).snapshotId()).build());
    validRefs.put("branch_b", SnapshotRef.branchBuilder(testSnapshots.get(2).snapshotId()).build());
    validRefs.put("branch_c", SnapshotRef.branchBuilder(testSnapshots.get(3).snapshotId()).build());

    // This should NOT throw an exception
    Assertions.assertDoesNotThrow(
        () ->
            openHouseInternalTableOperations.applySnapshotOperations(
                base, newSnapshots, validRefs, false),
        "Should NOT throw exception when branches target different snapshots");
  }

  @Test
  void testStandardWAPScenario() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    List<Snapshot> wapSnapshots = IcebergTestUtil.getWapSnapshots();

    // Create base with existing snapshots and a WAP snapshot
    TableMetadata base =
        TableMetadata.buildFrom(BASE_TABLE_METADATA)
            .setBranchSnapshot(testSnapshots.get(0), SnapshotRef.MAIN_BRANCH)
            .addSnapshot(wapSnapshots.get(0)) // WAP snapshot (not referenced by any branch)
            .build();

    // Standard WAP scenario: pull the WAP snapshot into main branch
    Snapshot wapSnapshot = wapSnapshots.get(0);
    List<Snapshot> newSnapshots = List.of(); // No new snapshots, just referencing the existing WAP

    // Create refs to pull WAP snapshot into main branch
    Map<String, SnapshotRef> refs = new HashMap<>();
    refs.put(SnapshotRef.MAIN_BRANCH, SnapshotRef.branchBuilder(wapSnapshot.snapshotId()).build());

    // Should succeed - standard WAP workflow where WAP snapshot becomes the new main
    Assertions.assertDoesNotThrow(
        () ->
            openHouseInternalTableOperations.applySnapshotOperations(
                base, newSnapshots, refs, false),
        "Should successfully pull WAP snapshot into main branch");
  }

  /**
   * Integration test that verifies committing with base and metadata that are at least two commits
   * divergent. This simulates scenarios where:
   *
   * <ul>
   *   <li>Base metadata is at version N
   *   <li>New metadata represents state at version N+2 or later (skipping intermediate versions)
   *   <li>The commit should still succeed and write complete metadata
   * </ul>
   *
   * <p>This test validates that Iceberg can handle "jump" commits where the metadata being
   * committed has evolved significantly from the base.
   */
  @Test
  void testMultipleDiffCommit() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {

      // ========== Create base at N with 1 snapshot ==========
      TableMetadata baseAtN =
          TableMetadata.buildFrom(BASE_TABLE_METADATA)
              .setBranchSnapshot(testSnapshots.get(0), SnapshotRef.MAIN_BRANCH)
              .build();

      // ========== Create divergent metadata at N+3 with 4 snapshots ==========
      // Simulate evolving through N+1 and N+2 without committing
      TableMetadata intermediate1 =
          TableMetadata.buildFrom(baseAtN)
              .setBranchSnapshot(testSnapshots.get(1), SnapshotRef.MAIN_BRANCH)
              .build();

      TableMetadata intermediate2 =
          TableMetadata.buildFrom(intermediate1)
              .setBranchSnapshot(testSnapshots.get(2), SnapshotRef.MAIN_BRANCH)
              .build();

      TableMetadata metadataAtNPlus3 =
          TableMetadata.buildFrom(intermediate2)
              .setBranchSnapshot(testSnapshots.get(3), SnapshotRef.MAIN_BRANCH)
              .build();

      // Add custom properties for commit
      Map<String, String> divergentProperties = new HashMap<>(metadataAtNPlus3.properties());
      List<Snapshot> snapshots4 = testSnapshots.subList(0, 4);
      divergentProperties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(snapshots4));
      divergentProperties.put(
          CatalogConstants.SNAPSHOTS_REFS_KEY,
          SnapshotsUtil.serializeMap(
              IcebergTestUtil.obtainSnapshotRefsFromSnapshot(snapshots4.get(3))));

      TableMetadata finalDivergentMetadata =
          metadataAtNPlus3.replaceProperties(divergentProperties);

      // ========== COMMIT: Base at N, Metadata at N+3 (divergent by 3 commits) ==========
      openHouseInternalTableOperations.doCommit(baseAtN, finalDivergentMetadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());

      TableMetadata capturedMetadata = tblMetadataCaptor.getValue();

      // Verify the divergent commit contains all 4 snapshots
      Assertions.assertEquals(
          4,
          capturedMetadata.snapshots().size(),
          "Divergent commit should contain all 4 snapshots despite jumping from base with 1 snapshot");

      Set<Long> expectedSnapshotIds =
          snapshots4.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
      Set<Long> actualSnapshotIds =
          capturedMetadata.snapshots().stream()
              .map(Snapshot::snapshotId)
              .collect(Collectors.toSet());
      Assertions.assertEquals(
          expectedSnapshotIds,
          actualSnapshotIds,
          "All snapshot IDs should be present after divergent commit");

      // Verify main ref points to the expected snapshot (the 4th snapshot)
      SnapshotRef mainRef = capturedMetadata.ref(SnapshotRef.MAIN_BRANCH);
      Assertions.assertNotNull(mainRef, "Main branch ref should exist");
      Assertions.assertEquals(
          testSnapshots.get(3).snapshotId(),
          mainRef.snapshotId(),
          "Main branch should point to the 4th snapshot after divergent commit");
    }
  }

  /**
   * Test committing with divergent metadata and multiple valid branches. Base is at N with MAIN,
   * metadata is at N+3 with both MAIN and feature_a branches pointing to different snapshots.
   */
  @Test
  void testMultipleDiffCommitWithValidBranch() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {

      // ========== Create base at N with 1 snapshot ==========
      TableMetadata baseAtN =
          TableMetadata.buildFrom(BASE_TABLE_METADATA)
              .setBranchSnapshot(testSnapshots.get(0), SnapshotRef.MAIN_BRANCH)
              .build();

      // ========== Create divergent metadata at N+3 with 4 snapshots and 2 branches ==========
      TableMetadata intermediate1 =
          TableMetadata.buildFrom(baseAtN)
              .setBranchSnapshot(testSnapshots.get(1), SnapshotRef.MAIN_BRANCH)
              .build();

      TableMetadata intermediate2 =
          TableMetadata.buildFrom(intermediate1)
              .setBranchSnapshot(testSnapshots.get(2), SnapshotRef.MAIN_BRANCH)
              .build();

      TableMetadata metadataAtNPlus3 =
          TableMetadata.buildFrom(intermediate2)
              .setBranchSnapshot(testSnapshots.get(3), SnapshotRef.MAIN_BRANCH)
              .build();

      // Add custom properties for commit with multiple branches
      Map<String, String> divergentProperties = new HashMap<>(metadataAtNPlus3.properties());
      List<Snapshot> snapshots4 = testSnapshots.subList(0, 4);
      divergentProperties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(snapshots4));

      // Create refs for both MAIN (pointing to snapshot 3) and feature_a (pointing to snapshot 2)
      Map<String, String> multipleRefs = new HashMap<>();
      multipleRefs.put(
          SnapshotRef.MAIN_BRANCH,
          SnapshotRefParser.toJson(
              SnapshotRef.branchBuilder(testSnapshots.get(3).snapshotId()).build()));
      multipleRefs.put(
          "feature_a",
          SnapshotRefParser.toJson(
              SnapshotRef.branchBuilder(testSnapshots.get(2).snapshotId()).build()));

      divergentProperties.put(
          CatalogConstants.SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(multipleRefs));

      TableMetadata finalDivergentMetadata =
          metadataAtNPlus3.replaceProperties(divergentProperties);

      // ========== COMMIT: Should succeed with multiple valid branches ==========
      openHouseInternalTableOperations.doCommit(baseAtN, finalDivergentMetadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());

      TableMetadata capturedMetadata = tblMetadataCaptor.getValue();

      // Verify all 4 snapshots are present
      Assertions.assertEquals(
          4,
          capturedMetadata.snapshots().size(),
          "Divergent commit with multiple branches should contain all 4 snapshots");

      // Verify main ref points to the expected snapshot
      SnapshotRef mainRef = capturedMetadata.ref(SnapshotRef.MAIN_BRANCH);
      Assertions.assertNotNull(mainRef, "Main branch ref should exist");
      Assertions.assertEquals(
          testSnapshots.get(3).snapshotId(),
          mainRef.snapshotId(),
          "Main branch should point to the 4th snapshot");

      // Verify feature_a ref points to the expected snapshot
      SnapshotRef featureRef = capturedMetadata.ref("feature_a");
      Assertions.assertNotNull(featureRef, "Feature_a branch ref should exist");
      Assertions.assertEquals(
          testSnapshots.get(2).snapshotId(),
          featureRef.snapshotId(),
          "Feature_a branch should point to the 3rd snapshot");
    }
  }

  /**
   * Test committing with divergent metadata where multiple branches point to the same snapshot.
   * This is VALID when done through setBranchSnapshot() - the end state is allowed.
   */
  @Test
  void testMultipleDiffCommitWithMultipleBranchesPointingToSameSnapshot() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {

      // ========== Create base at N with 1 snapshot ==========
      TableMetadata baseAtN =
          TableMetadata.buildFrom(BASE_TABLE_METADATA)
              .setBranchSnapshot(testSnapshots.get(0), SnapshotRef.MAIN_BRANCH)
              .build();

      // ========== Create divergent metadata with MAIN and feature_a both pointing to snapshot 3
      // ==========
      TableMetadata.Builder builder = TableMetadata.buildFrom(baseAtN);
      // Add snapshots 1, 2, 3 without assigning to branches
      builder.addSnapshot(testSnapshots.get(1));
      builder.addSnapshot(testSnapshots.get(2));
      builder.addSnapshot(testSnapshots.get(3));
      // Set BOTH branches to point to the same existing snapshot (using snapshot ID)
      builder.setBranchSnapshot(testSnapshots.get(3).snapshotId(), SnapshotRef.MAIN_BRANCH);
      builder.setBranchSnapshot(testSnapshots.get(3).snapshotId(), "feature_a");
      TableMetadata metadataWithBothBranches = builder.build();

      // Add custom properties with snapshots
      Map<String, String> divergentProperties =
          new HashMap<>(metadataWithBothBranches.properties());
      List<Snapshot> snapshots4 = testSnapshots.subList(0, 4);
      divergentProperties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(snapshots4));

      // Create refs matching the setBranchSnapshot calls - both pointing to snapshot 3
      Map<String, String> sameSnapshotRefs = new HashMap<>();
      sameSnapshotRefs.put(
          SnapshotRef.MAIN_BRANCH,
          SnapshotRefParser.toJson(
              SnapshotRef.branchBuilder(testSnapshots.get(3).snapshotId()).build()));
      sameSnapshotRefs.put(
          "feature_a",
          SnapshotRefParser.toJson(
              SnapshotRef.branchBuilder(testSnapshots.get(3).snapshotId()).build()));

      divergentProperties.put(
          CatalogConstants.SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(sameSnapshotRefs));

      TableMetadata finalDivergentMetadata =
          metadataWithBothBranches.replaceProperties(divergentProperties);

      // ========== COMMIT: Should SUCCEED - this is a valid end state ==========
      openHouseInternalTableOperations.doCommit(baseAtN, finalDivergentMetadata);
      Mockito.verify(mockHouseTableMapper).toHouseTable(tblMetadataCaptor.capture(), Mockito.any());

      TableMetadata capturedMetadata = tblMetadataCaptor.getValue();

      // Verify all 4 snapshots are present
      Assertions.assertEquals(
          4,
          capturedMetadata.snapshots().size(),
          "Commit with multiple branches pointing to same snapshot should contain all 4 snapshots");

      // Verify BOTH refs point to the same snapshot
      SnapshotRef mainRef = capturedMetadata.ref(SnapshotRef.MAIN_BRANCH);
      Assertions.assertNotNull(mainRef, "Main branch ref should exist");
      Assertions.assertEquals(
          testSnapshots.get(3).snapshotId(),
          mainRef.snapshotId(),
          "Main branch should point to the 4th snapshot");

      SnapshotRef featureRef = capturedMetadata.ref("feature_a");
      Assertions.assertNotNull(featureRef, "Feature_a branch ref should exist");
      Assertions.assertEquals(
          testSnapshots.get(3).snapshotId(),
          featureRef.snapshotId(),
          "Feature_a branch should also point to the 4th snapshot (same as main)");

      // Verify they point to the SAME snapshot
      Assertions.assertEquals(
          mainRef.snapshotId(),
          featureRef.snapshotId(),
          "Both branches should point to the same snapshot ID");
    }
  }

  /**
   * Test committing with divergent metadata where multiple branches try to point to the same
   * snapshot (ambiguous commit). This should throw an IllegalStateException.
   */
  @Test
  void testMultipleDiffCommitWithInvalidBranch() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();

    try (MockedStatic<TableMetadataParser> ignoreWriteMock =
        Mockito.mockStatic(TableMetadataParser.class)) {

      // ========== Create base at N with 1 snapshot ==========
      TableMetadata baseAtN =
          TableMetadata.buildFrom(BASE_TABLE_METADATA)
              .setBranchSnapshot(testSnapshots.get(0), SnapshotRef.MAIN_BRANCH)
              .build();

      // ========== Create metadata with 4 snapshots but only snapshot 0 in refs ==========
      // Build metadata with all 4 snapshots added, but keep MAIN pointing to snapshot 0
      TableMetadata.Builder builder = TableMetadata.buildFrom(baseAtN);
      // Add snapshots 1, 2, 3 without assigning them to any branch
      builder.addSnapshot(testSnapshots.get(1));
      builder.addSnapshot(testSnapshots.get(2));
      builder.addSnapshot(testSnapshots.get(3));
      TableMetadata metadataWithAllSnapshots = builder.build();

      // Add custom properties with AMBIGUOUS branch refs - both pointing to same snapshot
      Map<String, String> divergentProperties =
          new HashMap<>(metadataWithAllSnapshots.properties());
      List<Snapshot> snapshots4 = testSnapshots.subList(0, 4);
      divergentProperties.put(
          CatalogConstants.SNAPSHOTS_JSON_KEY, SnapshotsUtil.serializedSnapshots(snapshots4));

      // Create INVALID refs: both MAIN and feature_a pointing to the SAME snapshot (ambiguous!)
      Map<String, String> ambiguousRefs = new HashMap<>();
      ambiguousRefs.put(
          SnapshotRef.MAIN_BRANCH,
          SnapshotRefParser.toJson(
              SnapshotRef.branchBuilder(testSnapshots.get(3).snapshotId()).build()));
      ambiguousRefs.put(
          "feature_a",
          SnapshotRefParser.toJson(
              SnapshotRef.branchBuilder(testSnapshots.get(3).snapshotId())
                  .build())); // Same snapshot!

      divergentProperties.put(
          CatalogConstants.SNAPSHOTS_REFS_KEY, SnapshotsUtil.serializeMap(ambiguousRefs));

      TableMetadata finalDivergentMetadata =
          metadataWithAllSnapshots.replaceProperties(divergentProperties);

      // ========== COMMIT: Should throw CommitStateUnknownException due to ambiguous branches
      // ==========
      CommitStateUnknownException exception =
          Assertions.assertThrows(
              CommitStateUnknownException.class,
              () -> openHouseInternalTableOperations.doCommit(baseAtN, finalDivergentMetadata),
              "Should throw CommitStateUnknownException when multiple branches point to same snapshot");

      // Verify error message indicates the ambiguous commit
      String exceptionMessage = exception.getMessage();
      Assertions.assertTrue(
          exceptionMessage.contains("Multiple branches")
              && exceptionMessage.contains("same target snapshot"),
          "Error message should indicate multiple branches targeting same snapshot: "
              + exceptionMessage);
    }
  }
}
