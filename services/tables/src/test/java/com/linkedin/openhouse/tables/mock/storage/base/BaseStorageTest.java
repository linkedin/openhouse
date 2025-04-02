package com.linkedin.openhouse.tables.mock.storage.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.BaseStorage;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import com.linkedin.openhouse.cluster.storage.hdfs.HdfsStorageClient;
import java.util.HashMap;
import javax.annotation.PostConstruct;
import lombok.Setter;
import org.apache.hadoop.fs.FileSystem;
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

  private static final String DATABASE_ID = "db1";
  private static final String TABLE_ID = "table1";
  private static final String TABLE_UUID = "uuid1";
  private static final String TABLE_CREATOR = "creator1";

  @Test
  public void testAllocateTableLocationPattern1() {
    mockStorageProperties("hdfs://localhost:9000", "/data/openhouse");
    assertEquals(
        "hdfs://localhost:9000/data/openhouse/db1/table1-uuid1",
        baseStorage.allocateTableLocation(
            DATABASE_ID, TABLE_ID, TABLE_UUID, TABLE_CREATOR, new HashMap<>()));
  }

  @Test
  public void testAllocateTableLocationPattern2() {
    mockStorageProperties("hdfs://localhost:9000/", "/data/openhouse");
    assertEquals(
        "hdfs://localhost:9000/data/openhouse/db1/table1-uuid1",
        baseStorage.allocateTableLocation(
            DATABASE_ID, TABLE_ID, TABLE_UUID, TABLE_CREATOR, new HashMap<>()));
  }

  @Test
  public void testAllocateTableLocationPattern3() {
    mockStorageProperties("hdfs://localhost:9000/", "data/openhouse");
    assertEquals(
        "hdfs://localhost:9000/data/openhouse/db1/table1-uuid1",
        baseStorage.allocateTableLocation(
            DATABASE_ID, TABLE_ID, TABLE_UUID, TABLE_CREATOR, new HashMap<>()));
  }

  @Test
  public void testAllocateTableLocationPattern4() {
    mockStorageProperties("hdfs://localhost:9000/", "data");
    assertEquals(
        "hdfs://localhost:9000/data/db1/table1-uuid1",
        baseStorage.allocateTableLocation(
            DATABASE_ID, TABLE_ID, TABLE_UUID, TABLE_CREATOR, new HashMap<>()));
  }

  @Test
  public void testAllocateTableLocationPattern5() {
    mockStorageProperties("hdfs:///", "data/openhouse");
    assertEquals(
        "hdfs:///data/openhouse/db1/table1-uuid1",
        baseStorage.allocateTableLocation(
            DATABASE_ID, TABLE_ID, TABLE_UUID, TABLE_CREATOR, new HashMap<>()));
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
