package com.linkedin.openhouse.tables.mock.storage.hdfs;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import com.linkedin.openhouse.cluster.storage.hdfs.HdfsStorage;
import com.linkedin.openhouse.cluster.storage.hdfs.HdfsStorageClient;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest
public class HdfsStorageTest {
  @Autowired private HdfsStorage hdfsStorage;

  @MockBean private StorageProperties storageProperties;

  @MockBean private HdfsStorageClient hdfsStorageClient;

  @Test
  public void testHdfsStorageIsConfiguredWhenTypeIsProvided() {
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.HDFS.getValue(), new StorageProperties.StorageTypeProperties()));
    assertTrue(hdfsStorage.isConfigured());
  }

  @Test
  public void testHdfsStorageTypeIsCorrect() {
    assertEquals(StorageType.HDFS, hdfsStorage.getType());
  }

  @Test
  public void testHdfsStorageClientIsNotNull() {
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.HDFS.getValue(), new StorageProperties.StorageTypeProperties()));
    assertNotNull(hdfsStorageClient);
  }

  @Test
  public void testHdfsStoragePropertiesReturned() {
    Map<String, String> testMap = ImmutableMap.of("k1", "v1", "k2", "v2");
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.HDFS.getValue(),
                new StorageProperties.StorageTypeProperties(
                    "/data/openhouse", "hdfs://localhost:9000", testMap)));
    assertEquals(testMap, hdfsStorage.getProperties());
  }

  @Test
  public void testAllocateTableSpace() {
    String databaseId = "db1";
    String tableId = "table1";
    String tableUUID = "uuid1";
    String tableCreator = "creator1";
    boolean skipProvisioning = false;
    when(hdfsStorageClient.getRootPrefix()).thenReturn("/data/openhouse");
    when(hdfsStorageClient.getEndpoint()).thenReturn("hdfs://");
    String expected = "hdfs:///data/openhouse/db1/table1-uuid1";
    assertEquals(
        expected, hdfsStorage.allocateTableLocation(databaseId, tableId, tableUUID, tableCreator));
  }
}
