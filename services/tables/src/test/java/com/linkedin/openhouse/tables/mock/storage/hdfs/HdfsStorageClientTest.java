package com.linkedin.openhouse.tables.mock.storage.hdfs;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import com.linkedin.openhouse.cluster.storage.hdfs.HdfsStorageClient;
import java.util.HashMap;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;

@SpringBootTest
@Slf4j
public class HdfsStorageClientTest {

  @MockBean private StorageProperties storageProperties;

  @Autowired private ApplicationContext context;

  @Test
  public void tmp() {
    try {
      Object bean = context.getBean("storageManager");
      log.info("bean: {}", bean);
    } catch (Exception e) {
      log.error("ignore bean");
    }
  }

  private HdfsStorageClient hdfsStorageClient;

  @PostConstruct
  public void setupTest() {
    when(storageProperties.getDefaultType()).thenReturn(StorageType.HDFS.getValue());
    when(storageProperties.getTypes())
        .thenReturn(ImmutableMap.of(StorageType.HDFS.getValue(), getStorageTypeProperties()));
    hdfsStorageClient = context.getBean(HdfsStorageClient.class);
  }

  @Test
  public void testHdfsStorageClientInvalidPropertiesMissingRootPathAndEndpoint() {
    when(storageProperties.getTypes())
        .thenReturn(
            new HashMap<>(
                ImmutableMap.of(
                    StorageType.HDFS.getValue(), new StorageProperties.StorageTypeProperties())));
    assertThrows(IllegalArgumentException.class, () -> hdfsStorageClient.init());
  }

  @Test
  public void testHdfsStorageClientNullEmptyProperties() {
    when(storageProperties.getTypes()).thenReturn(null);
    assertThrows(IllegalArgumentException.class, () -> hdfsStorageClient.init());
    when(storageProperties.getTypes()).thenReturn(new HashMap<>());
    assertThrows(IllegalArgumentException.class, () -> hdfsStorageClient.init());
    when(storageProperties.getTypes())
        .thenReturn(
            new HashMap<String, StorageProperties.StorageTypeProperties>() {
              {
                put(StorageType.HDFS.getValue(), null);
              }
            });
    assertThrows(IllegalArgumentException.class, () -> hdfsStorageClient.init());
  }

  @Test
  public void testHdfsStorageClientValidProperties() {
    when(storageProperties.getTypes())
        .thenReturn(
            new HashMap<>(
                ImmutableMap.of(StorageType.HDFS.getValue(), getStorageTypeProperties())));
    assertDoesNotThrow(() -> hdfsStorageClient.init());
    assert hdfsStorageClient.getNativeClient() != null;
    assert hdfsStorageClient
        .getNativeClient()
        .getConf()
        .get("fs.defaultFS")
        .equals("hdfs://localhost:9000");
  }

  private StorageProperties.StorageTypeProperties getStorageTypeProperties() {
    StorageProperties.StorageTypeProperties storageTypeProperties =
        new StorageProperties.StorageTypeProperties();
    storageTypeProperties.setEndpoint("hdfs://localhost:9000");
    storageTypeProperties.setRootPath("/data/openhouse");
    storageTypeProperties.setParameters(new HashMap<>());
    return storageTypeProperties;
  }
}
