package com.linkedin.openhouse.tables.mock.storage.adls;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.adls.AdlsStorageClient;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;

@SpringBootTest
public class AdlsStorageClientTest {
  @MockBean private StorageProperties storageProperties;
  @Autowired private ApplicationContext context;
  private AdlsStorageClient adlsStorageClient;

  @PostConstruct
  public void setupTest() {
    when(storageProperties.getDefaultType()).thenReturn(StorageType.ADLS.getValue());
    when(storageProperties.getTypes())
        .thenReturn(ImmutableMap.of(StorageType.ADLS.getValue(), getStorageTypeProperties()));
    adlsStorageClient = context.getBean(AdlsStorageClient.class);
  }

  @Test
  public void testAdlsStorageClientInvalidProperties() {
    when(storageProperties.getTypes())
        .thenReturn(
            new HashMap<>(
                ImmutableMap.of(
                    StorageType.ADLS.getValue(), new StorageProperties.StorageTypeProperties())));
    assertThrows(IllegalArgumentException.class, () -> adlsStorageClient.init());
  }

  @Test
  public void testAdlsStorageClientNullOrEmptyProperties() {
    when(storageProperties.getTypes()).thenReturn(null);
    assertThrows(IllegalArgumentException.class, () -> adlsStorageClient.init());
    when(storageProperties.getTypes()).thenReturn(new HashMap<>());
    assertThrows(IllegalArgumentException.class, () -> adlsStorageClient.init());
    when(storageProperties.getTypes())
        .thenReturn(
            new HashMap<String, StorageProperties.StorageTypeProperties>() {
              {
                put(StorageType.ADLS.getValue(), null);
              }
            });
    assertThrows(IllegalArgumentException.class, () -> adlsStorageClient.init());
  }

  @Test
  public void testAdlsStorageClientValidProperties() {
    when(storageProperties.getTypes())
        .thenReturn(
            new HashMap<>(
                ImmutableMap.of(StorageType.ADLS.getValue(), getStorageTypeProperties())));
    assertDoesNotThrow(() -> adlsStorageClient.init());

    // TODO: Fix once DLFC gets initiated:
    // https://github.com/linkedin/openhouse/issues/148
    assert adlsStorageClient.getNativeClient() == null;

    assertEquals("abfs://", adlsStorageClient.getEndpoint());
    assertEquals(
        "testcontainer@teststorage.dfs.core.windows.net", adlsStorageClient.getRootPrefix());
  }

  private StorageProperties.StorageTypeProperties getStorageTypeProperties() {
    StorageProperties.StorageTypeProperties storageTypeProperties =
        new StorageProperties.StorageTypeProperties();
    storageTypeProperties.setEndpoint("abfs://");
    storageTypeProperties.setRootPath("testcontainer@teststorage.dfs.core.windows.net");
    Map<String, String> parameters = new HashMap<>();
    storageTypeProperties.setParameters(parameters);
    return storageTypeProperties;
  }
}
