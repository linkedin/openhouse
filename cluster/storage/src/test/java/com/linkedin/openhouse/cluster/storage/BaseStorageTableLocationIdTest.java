package com.linkedin.openhouse.cluster.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BaseStorageTableLocationIdTest {

  // Minimal subclass to test protected method
  static class TestStorage extends BaseStorage {
    @Override
    public StorageType.Type getType() {
      return StorageType.LOCAL;
    }

    @Override
    public StorageClient<?> getClient() {
      return null;
    }
  }

  @Test
  void testDefaultLocationId() {
    TestStorage storage = new TestStorage();
    Map<String, String> props = new HashMap<>();
    String tableId = "tableA";
    String tableUUID = "uuid123";
    String result = storage.calculateTableLocationId(tableId, tableUUID, props);
    assertEquals("tableA-uuid123", result);
  }

  @Test
  void testOverrideLocationId() {
    TestStorage storage = new TestStorage();
    Map<String, String> props = new HashMap<>();
    props.put("openhouse.replicaTableLocationId", "tableARenamed-123");
    String tableId = "tableA";
    String tableUUID = "uuid123";
    String result = storage.calculateTableLocationId(tableId, tableUUID, props);
    assertEquals("tableARenamed-123", result);
  }
}
