package com.linkedin.openhouse.tables.mock.storage.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import com.linkedin.openhouse.cluster.storage.local.LocalStorage;
import com.linkedin.openhouse.cluster.storage.local.LocalStorageClient;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest
public class LocalStorageTest {

  @Autowired private LocalStorage localStorage;

  @MockBean private StorageProperties storageProperties;

  @MockBean private LocalStorageClient localStorageClient;

  private static final String DEFAULT_TYPE = "hdfs";

  @Test
  public void testLocalStorageIsConfiguredWhenDefaultTypeIsNull() {
    when(storageProperties.getDefaultType()).thenReturn(null);
    boolean result = localStorage.isConfigured();
    assertTrue(result);
  }

  @Test
  public void testLocalStorageIsConfiguredWhenTypesIsNull() {
    when(storageProperties.getDefaultType()).thenReturn(DEFAULT_TYPE);
    when(storageProperties.getTypes()).thenReturn(null);
    boolean result = localStorage.isConfigured();
    assertTrue(result);
  }

  @Test
  public void testLocalStorageIsConfiguredWhenTypesIsEmpty() {
    when(storageProperties.getDefaultType()).thenReturn(DEFAULT_TYPE);
    when(storageProperties.getTypes()).thenReturn(new HashMap<>());
    boolean result = localStorage.isConfigured();
    assertTrue(result);
  }

  @Test
  public void testLocalStorageIsConfiguredWhenTypesContainsType() {
    when(storageProperties.getDefaultType()).thenReturn(DEFAULT_TYPE);
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.LOCAL.getValue(), new StorageProperties.StorageTypeProperties()));
    boolean result = localStorage.isConfigured();
    assertTrue(result);
  }

  @Test
  public void testLocalStorageGetProperties() {
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.LOCAL.getValue(),
                new StorageProperties.StorageTypeProperties(
                    "rootPath", "endPoint", ImmutableMap.of("key", "value"))));
    assertTrue(localStorage.getProperties().containsKey("key"));
    assertTrue(localStorage.getProperties().containsValue("value"));
  }

  @Test
  public void testLocalStorageGetType() {
    assertTrue(localStorage.getType().equals(StorageType.LOCAL));
  }

  @Test
  public void testLocalStorageGetClient() {
    LocalFileSystem localFileSystem = new LocalFileSystem();
    when(localStorageClient.getNativeClient()).thenReturn(localFileSystem);
    assertTrue(localStorage.getClient().getNativeClient().equals(localFileSystem));
  }

  @Test
  public void testAllocateTableSpace() {
    String databaseId = "db1";
    String tableId = "table1";
    String tableUUID = "uuid1";
    String tableCreator = "creator1";
    boolean skipProvisioning = false;
    when(localStorageClient.getRootPrefix()).thenReturn("/tmp");
    String expected = "/tmp/db1/table1-uuid1";
    assertEquals(
        expected, localStorage.allocateTableLocation(databaseId, tableId, tableUUID, tableCreator));
  }

  @Test
  public void testDeallocateTableLocation() throws IOException {

    String tableLocation = String.format("%s/db/table", this.getClass().getSimpleName());
    FileSystem fs = FileSystem.get(new Configuration());
    if (fs.exists(new Path(tableLocation))) {
      fs.delete(new Path(tableLocation), true);
    }
    fs.mkdirs(new Path(tableLocation));
    Assertions.assertTrue(fs.exists(new Path(tableLocation)));
    String metadataLocation = String.format("%s/metadata", tableLocation);
    fs.mkdirs(new Path(metadataLocation));
    Assertions.assertTrue(fs.exists(new Path(metadataLocation)));
    String dataLocation = String.format("%s/data", tableLocation);
    fs.mkdirs(new Path(dataLocation));
    Assertions.assertTrue(fs.exists(new Path(dataLocation)));
    when(localStorageClient.getNativeClient()).thenReturn(fs);
    localStorage.deallocateTableLocation(tableLocation, "tableCreator");
    Assertions.assertFalse(fs.exists(new Path(tableLocation)));
    Assertions.assertFalse(fs.exists(new Path(metadataLocation)));
    Assertions.assertFalse(fs.exists(new Path(dataLocation)));
  }
}
