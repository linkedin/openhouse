package com.linkedin.openhouse.cluster.storage.selector.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class RegexStorageSelectorTest {
  @Mock private StorageManager storageManager;
  @Mock private StorageProperties storageProperties;
  @Mock private StorageType storageType;
  @Mock private StorageType.Type defaultStorageType;
  @Mock private Storage defaultStorage;
  @Mock private Storage providedStorage;
  @Mock private StorageProperties.StorageSelectorProperties storageSelectorProperties;
  @InjectMocks private RegexStorageSelector storageSelector;
  private Map<String, String> selectorParams;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);

    // Mock the default storage
    when(storageManager.getDefaultStorage()).thenReturn(defaultStorage);
    when(defaultStorage.getType()).thenReturn(defaultStorageType);
    when(defaultStorageType.getValue()).thenReturn("defaultStorage");

    // Mock the provided storage
    when(storageManager.getStorage(any())).thenReturn(providedStorage);
    StorageType.Type type = mock(StorageType.Type.class);
    when(storageType.fromString(eq("providedStorage"))).thenReturn(type);

    // Prepare the storage properties and parameters
    when(storageProperties.getStorageSelector()).thenReturn(storageSelectorProperties);
    // Properly set the name of the selector in the mock
    when(storageSelectorProperties.getName())
        .thenReturn(RegexStorageSelector.class.getSimpleName());

    selectorParams = new HashMap<>();
    when(storageSelectorProperties.getParameters()).thenReturn(selectorParams);

    storageSelector.storageProperties = storageProperties;
    storageSelector.storageType = storageType;
    storageSelector.storageManager = storageManager;
  }

  @Test
  void testInit_MissingRegex_ThrowsException() {
    // Arrange: only storage-type is present
    selectorParams.put("storage-type", "providedStorage");

    // Act & Assert: ensure that a missing regex throws a NullPointerException
    assertThrows(NullPointerException.class, () -> storageSelector.init());
  }

  @Test
  void testInit_MissingStorageType_ThrowsException() {
    // Arrange: only regex is present
    selectorParams.put("regex", "db1\\.table1");

    // Act & Assert: ensure that a missing storage-type throws a NullPointerException
    assertThrows(NullPointerException.class, () -> storageSelector.init());
  }

  @Test
  void testSelectStorage_RegexMatches_ReturnsProvidedStorage() {
    // Arrange: setup regex and storage type
    selectorParams.put("regex", ".*prod.*\\.table.*");
    selectorParams.put("storage-type", "providedStorage");

    // Initialize
    storageSelector.init();

    Storage selectedStorage1 = storageSelector.selectStorage("db_prod", "table1");
    Storage selectedStorage2 = storageSelector.selectStorage("another_prod_db", "table99");

    assertEquals(providedStorage, selectedStorage1);
    assertEquals(providedStorage, selectedStorage2);
    // 2 calls to getStorage
    verify(storageManager, times(2)).getStorage(any());
  }

  @Test
  void testSelectStorage_RegexDoesNotMatch_ReturnsDefaultStorage() {
    // Arrange: setup regex and storage type
    selectorParams.put("regex", "db1\\.table1");
    selectorParams.put("storage-type", "providedStorage");

    // Initialize
    storageSelector.init();

    // Assert: ensure default storage is returned
    assertEquals(defaultStorage, storageSelector.selectStorage("db2", "table2"));
    assertEquals(defaultStorage, storageSelector.selectStorage("prod_db", "table1"));
    // No call to getStorage
    verify(storageManager, never()).getStorage(any());
  }

  @Test
  void testSelectStorage_ReturnsProvidedAndDefaultStorage() {
    // Arrange: setup regex and storage type
    selectorParams.put("regex", "db1\\.table1");
    selectorParams.put("storage-type", "providedStorage");

    // Initialize
    storageSelector.init();

    Storage selectedStorage1 = storageSelector.selectStorage("db1", "table1");
    Storage selectedStorage2 = storageSelector.selectStorage("db1", "table2");

    assertEquals(providedStorage, selectedStorage1);
    assertEquals(defaultStorage, selectedStorage2);
  }
}
