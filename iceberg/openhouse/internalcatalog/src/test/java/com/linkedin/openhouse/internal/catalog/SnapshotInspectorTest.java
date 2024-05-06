package com.linkedin.openhouse.internal.catalog;

import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.internal.catalog.exception.InvalidIcebergSnapshotException;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOConfig;
import com.linkedin.openhouse.internal.catalog.fileio.FileIOManager;
import com.linkedin.openhouse.internal.catalog.mapper.HouseTableMapperTest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(HouseTableMapperTest.MockConfiguration.class)
class SnapshotInspectorTest {

  @Autowired SnapshotInspector snapshotInspector;

  @TempDir static Path tempDir;

  @MockBean StorageManager storageManager;

  @MockBean FileIOManager fileIOManager;

  @MockBean FileIOConfig fileIOConfig;
  private static final TableMetadata noSnapshotsMetadata =
      TableMetadata.newTableMetadata(
          new Schema(
              Types.NestedField.required(1, "data", Types.StringType.get()),
              Types.NestedField.required(2, "ts", Types.TimestampType.withoutZone())),
          PartitionSpec.unpartitioned(),
          UUID.randomUUID().toString(),
          ImmutableMap.of());

  @Test
  void testValidateSnapshotsUpdateWithNoSnapshotMetadata() throws IOException {

    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    // No exception since added as well deleted snapshots are allowed to support replication
    // use case which performs table commit with added and deleted snapshots.
    Assertions.assertDoesNotThrow(
        () ->
            snapshotInspector.validateSnapshotsUpdate(
                noSnapshotsMetadata, testSnapshots.subList(0, 1), testSnapshots.subList(1, 4)));
    Assertions.assertDoesNotThrow(
        () ->
            snapshotInspector.validateSnapshotsUpdate(
                noSnapshotsMetadata, testSnapshots, Collections.emptyList()));
    Assertions.assertDoesNotThrow(
        () ->
            snapshotInspector.validateSnapshotsUpdate(
                noSnapshotsMetadata, Collections.emptyList(), testSnapshots));
  }

  @Test
  void testValidateSnapshotsUpdateWithSnapshotMetadata() throws IOException {
    List<Snapshot> testSnapshots = IcebergTestUtil.getSnapshots();
    List<Snapshot> extraTestSnapshots = IcebergTestUtil.getExtraSnapshots();
    TableMetadata metadataWithSnapshots =
        TableMetadata.buildFrom(noSnapshotsMetadata)
            .setBranchSnapshot(testSnapshots.get(testSnapshots.size() - 1), SnapshotRef.MAIN_BRANCH)
            .build();
    Assertions.assertDoesNotThrow(
        () ->
            snapshotInspector.validateSnapshotsUpdate(
                metadataWithSnapshots, testSnapshots, Collections.emptyList()));
    // No validation error if snapshots are added and deleted
    Assertions.assertDoesNotThrow(
        () ->
            snapshotInspector.validateSnapshotsUpdate(
                metadataWithSnapshots, testSnapshots, testSnapshots));
    // No validation error if snapshots are added and deleted
    Assertions.assertDoesNotThrow(
        () ->
            snapshotInspector.validateSnapshotsUpdate(
                metadataWithSnapshots, extraTestSnapshots, testSnapshots));
    Assertions.assertThrows(
        InvalidIcebergSnapshotException.class,
        () ->
            snapshotInspector.validateSnapshotsUpdate(
                metadataWithSnapshots, Collections.emptyList(), testSnapshots));
    Assertions.assertDoesNotThrow(
        () ->
            snapshotInspector.validateSnapshotsUpdate(
                metadataWithSnapshots,
                Collections.emptyList(),
                testSnapshots.subList(0, testSnapshots.size() - 1)));
  }

  @Test
  void testSecureSnapshot() throws IOException {
    // The default file attribute that sets the permission as 777 when a file is created.
    FileAttribute<Set<PosixFilePermission>> attr =
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx"));

    // Mock DataFile and ManifestFile
    Snapshot mockSnapshot = Mockito.mock(org.apache.iceberg.Snapshot.class);
    Path tempFile1 = Files.createFile(tempDir.resolve("data1.parquet"), attr);
    Path tempFile2 = Files.createFile(tempDir.resolve("data2.parquet"), attr);
    Path tempFile3 = Files.createFile(tempDir.resolve("manifest"), attr);

    // Mock FileIO
    FileIO fileIO = Mockito.mock(org.apache.iceberg.io.FileIO.class);

    List<DataFile> dataFileList =
        ImmutableList.of(
            createDataFile(tempFile1.toString()), createDataFile(tempFile2.toString()));

    ManifestWriter<DataFile> manifestWriter =
        ManifestFiles.write(
            PartitionSpec.unpartitioned(),
            HadoopOutputFile.fromLocation(tempFile3.toString(), new Configuration()));
    manifestWriter.close();

    Mockito.when(mockSnapshot.allManifests(fileIO))
        .thenReturn(ImmutableList.of(manifestWriter.toManifestFile()));
    Mockito.when(mockSnapshot.addedDataFiles(fileIO)).thenReturn(dataFileList);
    snapshotInspector.secureSnapshot(mockSnapshot, fileIO);

    /* Verify the perms of files are modified as com.linkedin.openhouse.internal.catalog.MockApplication.perm does */
    FileSystem fileSystem = FileSystem.get(new Configuration());
    Assertions.assertEquals(
        fileSystem
            .getFileStatus(new org.apache.hadoop.fs.Path(tempFile1.toString()))
            .getPermission(),
        MockApplication.perm);
    Assertions.assertEquals(
        fileSystem
            .getFileStatus(new org.apache.hadoop.fs.Path(tempFile2.toString()))
            .getPermission(),
        MockApplication.perm);
    Assertions.assertEquals(
        fileSystem
            .getFileStatus(new org.apache.hadoop.fs.Path(tempFile3.toString()))
            .getPermission(),
        MockApplication.perm);
  }

  public static DataFile createDataFile(String dataPath) throws IOException {
    Files.write(Paths.get(dataPath), Lists.newArrayList(), StandardCharsets.UTF_8);
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(dataPath)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }
}
