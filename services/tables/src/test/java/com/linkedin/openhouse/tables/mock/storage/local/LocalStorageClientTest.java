package com.linkedin.openhouse.tables.mock.storage.local;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import com.linkedin.openhouse.cluster.storage.local.LocalStorageClient;
import java.util.Collections;
import java.util.HashMap;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;

@SpringBootTest
@Slf4j
public class LocalStorageClientTest {

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

  private LocalStorageClient localStorageClient;

  @PostConstruct
  public void setupTest() {
    when(storageProperties.getTypes()).thenReturn(null);
    localStorageClient = context.getBean(LocalStorageClient.class);
  }

  @Test
  public void testLocalStorageClientInvalidPropertiesMissingRootPathAndEndpoint() {
    when(storageProperties.getTypes())
        .thenReturn(
            new HashMap<>(
                Collections.singletonMap(
                    StorageType.LOCAL.getValue(), new StorageProperties.StorageTypeProperties())));
    assertThrows(IllegalArgumentException.class, () -> localStorageClient.init());
  }

  @Test
  public void testLocalStorageClientNullProperties() {
    when(storageProperties.getTypes()).thenReturn(null);
    assertDoesNotThrow(() -> localStorageClient.init());
  }

  @Test
  public void testLocalStorageClientEmptyMap() {
    when(storageProperties.getTypes()).thenReturn(new HashMap<>());
    assertDoesNotThrow(() -> localStorageClient.init());
  }

  @Test
  public void testLocalStorageClientValidProperties() {
    when(storageProperties.getTypes())
        .thenReturn(
            new HashMap<>(
                Collections.singletonMap(
                    StorageType.LOCAL.getValue(),
                    new StorageProperties.StorageTypeProperties(
                        "/tmp2", "file://", new HashMap<>()))));
    assertDoesNotThrow(() -> localStorageClient.init());
  }

  @Test
  public void testLocalStorageClientInValidEndpoint() {
    when(storageProperties.getTypes())
        .thenReturn(
            new HashMap<>(
                Collections.singletonMap(
                    StorageType.LOCAL.getValue(),
                    new StorageProperties.StorageTypeProperties(
                        "/tmp", "s3://", new HashMap<>()))));
    assertThrows(IllegalArgumentException.class, () -> localStorageClient.init());
  }

  @Test
  public void testLocalStorageClientInitialized() throws Exception {
    when(storageProperties.getTypes()).thenReturn(null);
    localStorageClient.init();
    Object client = localStorageClient.getNativeClient();
    assert client != null;
    assert client instanceof LocalFileSystem;
  }

  @Test
  public void testLocalStorageCanCreateFile() throws Exception {
    java.util.Random random = new java.util.Random();
    String tempFile = String.format("/tmp/testFile%s.orc", Math.abs(random.nextInt()));
    when(storageProperties.getTypes())
        .thenReturn(
            new HashMap<>(
                Collections.singletonMap(
                    StorageType.LOCAL.getValue(),
                    new StorageProperties.StorageTypeProperties(
                        "/tmp", "file://", new HashMap<>()))));
    localStorageClient.init();
    assert localStorageClient
        .getNativeClient()
        .createNewFile(new org.apache.hadoop.fs.Path(tempFile));
    assert localStorageClient.getNativeClient().exists(new org.apache.hadoop.fs.Path(tempFile));
    assert localStorageClient
        .getNativeClient()
        .delete(new org.apache.hadoop.fs.Path(tempFile), false);
  }
}
