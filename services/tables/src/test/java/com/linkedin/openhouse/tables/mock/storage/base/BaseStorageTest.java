package com.linkedin.openhouse.tables.mock.storage.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.BaseStorage;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import com.linkedin.openhouse.cluster.storage.hdfs.HdfsStorageClient;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.PostConstruct;
import lombok.Setter;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest
public class BaseStorageTest {

  @MockBean private StorageProperties storageProperties;

  @MockBean private HdfsStorageClient hdfsStorageClient;

  @TestComponent
  @Setter
  public static class DummyBaseStorage extends BaseStorage {

    HdfsStorageClient hdfsStorageClient;

    @Override
    public StorageType.Type getType() {
      return new StorageType.Type("TEST"); // return a dummy type
    }

    @Override
    public StorageClient<FileSystem> getClient() {
      return hdfsStorageClient; // return a dummy client
    }
  }

  @Autowired private DummyBaseStorage baseStorage;

  @PostConstruct
  public void setupTest() {
    baseStorage.setHdfsStorageClient(hdfsStorageClient);
  }

  private static final String databaseId = "db1";
  private static final String tableId = "table1";
  private static final String tableUUID = "uuid1";
  private static final String tableCreator = "creator1";

  @Test
  public void testAllocateTableLocationPattern1() {
    mockStorageProperties("hdfs://localhost:9000", "/data/openhouse");
    assertEquals(
        "hdfs://localhost:9000/data/openhouse/db1/table1-uuid1",
        baseStorage.allocateTableLocation(databaseId, tableId, tableUUID, tableCreator));
  }

  @Test
  public void testAllocateTableLocationPattern2() {
    mockStorageProperties("hdfs://localhost:9000/", "/data/openhouse");
    assertEquals(
        "hdfs://localhost:9000/data/openhouse/db1/table1-uuid1",
        baseStorage.allocateTableLocation(databaseId, tableId, tableUUID, tableCreator));
  }

  @Test
  public void testAllocateTableLocationPattern3() {
    mockStorageProperties("hdfs://localhost:9000/", "data/openhouse");
    assertEquals(
        "hdfs://localhost:9000/data/openhouse/db1/table1-uuid1",
        baseStorage.allocateTableLocation(databaseId, tableId, tableUUID, tableCreator));
  }

  @Test
  public void testAllocateTableLocationPattern4() {
    mockStorageProperties("hdfs://localhost:9000/", "data");
    assertEquals(
        "hdfs://localhost:9000/data/db1/table1-uuid1",
        baseStorage.allocateTableLocation(databaseId, tableId, tableUUID, tableCreator));
  }

  @Test
  public void testAllocateTableLocationPattern5() {
    mockStorageProperties("hdfs:///", "data/openhouse");
    assertEquals(
        "hdfs:///data/openhouse/db1/table1-uuid1",
        baseStorage.allocateTableLocation(databaseId, tableId, tableUUID, tableCreator));
  }

  @Test
  public void testDeallocateTableLocation() throws IOException {

    Path dir = Files.createTempDirectory("test");
    // Create some files and subdirectories within the test directory
    Files.createFile(dir.resolve("file1.txt"));
    Files.createFile(dir.resolve("file2.txt"));
    Path subdir = Files.createDirectory(dir.resolve("subdir"));
    Files.createFile(dir.resolve("subdir").resolve("file3.txt"));

    baseStorage.deallocateTableLocation(dir.toString(), "tableCreator");
    Assertions.assertFalse(Files.exists(dir));
    Assertions.assertFalse(Files.exists(subdir));
  }

  void mockStorageProperties(String endpoint, String rootPrefix) {
    when(hdfsStorageClient.getEndpoint()).thenReturn(endpoint);
    when(hdfsStorageClient.getRootPrefix()).thenReturn(rootPrefix);
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                "TEST",
                new StorageProperties.StorageTypeProperties(
                    rootPrefix, endpoint, ImmutableMap.of())));
  }
}
