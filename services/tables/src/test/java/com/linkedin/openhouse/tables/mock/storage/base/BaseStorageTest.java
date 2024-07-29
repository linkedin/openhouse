package com.linkedin.openhouse.tables.mock.storage.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.BaseStorage;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import com.linkedin.openhouse.cluster.storage.hdfs.HdfsStorageClient;
import javax.annotation.PostConstruct;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;

@SpringBootTest
@Slf4j
public class BaseStorageTest {

  @MockBean private StorageProperties storageProperties;

  @MockBean private HdfsStorageClient hdfsStorageClient;

  @Autowired private ApplicationContext appContext;

  @Test
  public void tmp() {
    try {
      Object bean = appContext.getBean("storageManager");
      log.info("bean: {}", bean);
    } catch (Exception e) {
      log.error("ignore bean");
    }
  }

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
