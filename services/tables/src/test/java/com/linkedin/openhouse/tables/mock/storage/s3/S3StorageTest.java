package com.linkedin.openhouse.tables.mock.storage.s3;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import com.linkedin.openhouse.cluster.storage.s3.S3Storage;
import com.linkedin.openhouse.cluster.storage.s3.S3StorageClient;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;

@SpringBootTest
@Slf4j
public class S3StorageTest {
  @Autowired private S3Storage s3Storage;

  @MockBean private StorageProperties storageProperties;

  @MockBean private S3StorageClient s3StorageClient;

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

  @Test
  public void testS3StorageIsConfiguredWhenTypeIsProvided() {
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.S3.getValue(), new StorageProperties.StorageTypeProperties()));
    assertTrue(s3Storage.isConfigured());
  }

  @Test
  public void testS3StorageTypeIsCorrect() {
    assertEquals(StorageType.S3, s3Storage.getType());
  }

  @Test
  public void testS3StorageClientIsNotNull() {
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.S3.getValue(), new StorageProperties.StorageTypeProperties()));
    assertNotNull(s3StorageClient);
  }

  @Test
  public void testS3StoragePropertiesReturned() {
    Map<String, String> testMap = ImmutableMap.of("k1", "v1", "k2", "v2");
    System.setProperty("aws.region", "us-east-1");
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.S3.getValue(),
                new StorageProperties.StorageTypeProperties(
                    "/mybucket", "http://S3:9000", testMap)));
    assertEquals(testMap, s3Storage.getProperties());
  }

  @Test
  public void testAllocateTableSpace() {
    String databaseId = "db1";
    String tableId = "table1";
    String tableUUID = "uuid1";
    String tableCreator = "creator1";
    boolean skipProvisioning = false;
    Map<String, String> testMap = ImmutableMap.of("k1", "v1", "k2", "v2");
    System.setProperty("aws.region", "us-east-1");
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.S3.getValue(),
                new StorageProperties.StorageTypeProperties("mybucket", "S3://", testMap)));
    when(s3StorageClient.getEndpoint()).thenReturn("S3://");
    when(s3StorageClient.getRootPrefix()).thenReturn("mybucket");
    String expected = "S3://mybucket/db1/table1-uuid1";
    assertEquals(
        expected, s3Storage.allocateTableLocation(databaseId, tableId, tableUUID, tableCreator));
  }
}
