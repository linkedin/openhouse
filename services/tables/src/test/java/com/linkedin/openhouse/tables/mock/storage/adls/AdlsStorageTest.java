package com.linkedin.openhouse.tables.mock.storage.adls;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.adls.AdlsStorage;
import com.linkedin.openhouse.cluster.storage.adls.AdlsStorageClient;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest
public class AdlsStorageTest {
  @Autowired private AdlsStorage adlsStorage;

  @MockBean private StorageProperties storageProperties;

  @MockBean private AdlsStorageClient adlsStorageClient;

  @Test
  public void testAdlsStorageIsConfiguredWhenTypeIsProvided() {
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.ADLS.getValue(), new StorageProperties.StorageTypeProperties()));
    assertTrue(adlsStorage.isConfigured());
  }

  @Test
  public void testAdlsStorageTypeIsCorrect() {
    assertEquals(StorageType.ADLS, adlsStorage.getType());
  }

  @Test
  public void testAdlsStorageClientIsNotNull() {
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.ADLS.getValue(), new StorageProperties.StorageTypeProperties()));
    assertNotNull(adlsStorageClient);
  }

  @Test
  public void testAdlsStoragePropertiesReturned() {
    Map<String, String> testMap = ImmutableMap.of("k1", "v1", "k2", "v2");
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.ADLS.getValue(),
                new StorageProperties.StorageTypeProperties(
                    "abfs://", "testcontainer@teststorage.dfs.core.windows.net", testMap)));
    assertEquals(testMap, adlsStorage.getProperties());
  }

  @Test
  public void testAllocateTableSpace() {
    String databaseId = "db1";
    String tableId = "table1";
    String tableUUID = "uuid1";
    String tableCreator = "creator1";
    boolean skipProvisioning = false;
    Map<String, String> testMap = ImmutableMap.of("k1", "v1", "k2", "v2");
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.ADLS.getValue(),
                new StorageProperties.StorageTypeProperties(
                    "abfs://", "testcontainer@teststorage.dfs.core.windows.net", testMap)));
    when(adlsStorageClient.getEndpoint()).thenReturn("abfs://");
    when(adlsStorageClient.getRootPrefix())
        .thenReturn("testcontainer@teststorage.dfs.core.windows.net");
    String expected = "abfs://testcontainer@teststorage.dfs.core.windows.net/db1/table1-uuid1";
    assertEquals(
        expected, adlsStorage.allocateTableLocation(databaseId, tableId, tableUUID, tableCreator));
  }
}
