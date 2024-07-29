package com.linkedin.openhouse.tables.mock.storage.s3;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import com.linkedin.openhouse.cluster.storage.s3.S3StorageClient;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;

@SpringBootTest
@Slf4j
public class S3StorageClientTest {
  @MockBean private StorageProperties storageProperties;
  @Autowired private ApplicationContext context;
  private S3StorageClient s3StorageClient;

  @Test
  public void tmp() {
    try {
      Object bean = context.getBean("storageManager");
      log.info("bean: {}", bean);
    } catch (Exception e) {
      log.error("ignore bean");
    }
  }

  @PostConstruct
  public void setupTest() {
    when(storageProperties.getDefaultType()).thenReturn(StorageType.S3.getValue());
    when(storageProperties.getTypes())
        .thenReturn(ImmutableMap.of(StorageType.S3.getValue(), getStorageTypeProperties()));
    s3StorageClient = context.getBean(S3StorageClient.class);
  }

  @Test
  public void testS3StorageClientInvalidProperties() {
    when(storageProperties.getTypes())
        .thenReturn(
            new HashMap<>(
                ImmutableMap.of(
                    StorageType.S3.getValue(), new StorageProperties.StorageTypeProperties())));
    assertThrows(IllegalArgumentException.class, () -> s3StorageClient.init());
  }

  @Test
  public void testS3StorageClientNullOrEmptyProperties() {
    when(storageProperties.getTypes()).thenReturn(null);
    assertThrows(IllegalArgumentException.class, () -> s3StorageClient.init());
    when(storageProperties.getTypes()).thenReturn(new HashMap<>());
    assertThrows(IllegalArgumentException.class, () -> s3StorageClient.init());
    when(storageProperties.getTypes())
        .thenReturn(
            new HashMap<String, StorageProperties.StorageTypeProperties>() {
              {
                put(StorageType.S3.getValue(), null);
              }
            });
    assertThrows(IllegalArgumentException.class, () -> s3StorageClient.init());
  }

  @Test
  public void testS3StorageClientValidProperties() {
    when(storageProperties.getTypes())
        .thenReturn(
            new HashMap<>(ImmutableMap.of(StorageType.S3.getValue(), getStorageTypeProperties())));
    assertDoesNotThrow(() -> s3StorageClient.init());
    assert s3StorageClient.getNativeClient() != null;
    assertEquals("http://S3:9000", s3StorageClient.getEndpoint());
    assertEquals("/mybucket", s3StorageClient.getRootPrefix());
  }

  private StorageProperties.StorageTypeProperties getStorageTypeProperties() {
    StorageProperties.StorageTypeProperties storageTypeProperties =
        new StorageProperties.StorageTypeProperties();
    storageTypeProperties.setEndpoint("http://S3:9000");
    storageTypeProperties.setRootPath("/mybucket");
    Map<String, String> parameters = new HashMap<>();
    System.setProperty("aws.region", "us-east-1");
    storageTypeProperties.setParameters(parameters);
    return storageTypeProperties;
  }
}
