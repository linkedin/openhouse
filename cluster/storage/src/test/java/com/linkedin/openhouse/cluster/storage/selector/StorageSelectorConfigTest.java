package com.linkedin.openhouse.cluster.storage.selector;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import com.linkedin.openhouse.cluster.storage.hdfs.HdfsStorage;
import com.linkedin.openhouse.cluster.storage.s3.S3Storage;
import com.linkedin.openhouse.cluster.storage.selector.impl.DefaultStorageSelector;
import com.linkedin.openhouse.cluster.storage.selector.impl.RegexStorageSelector;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class StorageSelectorConfigTest {

  @Mock private StorageProperties storageProperties;

  @Mock private StorageProperties.StorageSelectorProperties storageSelectorProperties;

  @Mock private StorageSelector selector1;

  @Mock private StorageSelector selector2;

  @Mock private DefaultStorageSelector defaultStorageSelector;

  @Mock private RegexStorageSelector regexStorageSelector;

  @InjectMocks private StorageSelectorConfig storageSelectorConfig;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testProvideStorageSelectorFound() {
    when(storageSelectorProperties.getName()).thenReturn("selector1");
    when(storageSelectorProperties.getParameters())
        .thenReturn(ImmutableMap.of("key1", "value1", "key2", "value2"));
    when(storageProperties.getStorageSelector()).thenReturn(storageSelectorProperties);
    when(selector1.getName()).thenReturn("selector1");
    when(selector2.getName()).thenReturn("selector2");
    storageSelectorConfig.storageSelectors = Arrays.asList(selector1, selector2);

    assertEquals(selector1, storageSelectorConfig.provideStorageSelector());
  }

  @Test
  public void testProvideStorageSelectorNotFound() {
    when(storageSelectorProperties.getName()).thenReturn("selector3");
    when(storageProperties.getStorageSelector()).thenReturn(storageSelectorProperties);
    when(selector1.getName()).thenReturn("selector1");
    when(selector2.getName()).thenReturn("selector2");
    storageSelectorConfig.storageSelectors = Arrays.asList(selector1, selector2);

    assertThrows(
        IllegalArgumentException.class, () -> storageSelectorConfig.provideStorageSelector());
  }

  @Test
  public void testProvideStorageSelectorMissingConfig() {
    when(selector1.getName()).thenReturn("selector1");
    when(selector2.getName()).thenReturn("selector2");
    when(defaultStorageSelector.getName()).thenReturn(DefaultStorageSelector.class.getSimpleName());
    storageSelectorConfig.defaultStorageSelector = defaultStorageSelector;
    storageSelectorConfig.storageSelectors = Arrays.asList(selector1, selector2);

    assertEquals(
        DefaultStorageSelector.class.getSimpleName(),
        storageSelectorConfig.provideStorageSelector().getName());
  }

  @Test
  public void testProvideRegexStorageSelector() {

    when(storageSelectorProperties.getName())
        .thenReturn(RegexStorageSelector.class.getSimpleName());
    when(storageSelectorProperties.getParameters())
        .thenReturn(ImmutableMap.of("regex", "db.*\\.table.*", "storage-type", "custom"));
    when(storageProperties.getStorageSelector()).thenReturn(storageSelectorProperties);

    // Regex selector returns s3 storage
    when(regexStorageSelector.getName()).thenReturn(RegexStorageSelector.class.getSimpleName());
    Storage s3Storage = mock(S3Storage.class);
    when(regexStorageSelector.selectStorage(anyString(), anyString())).thenReturn(s3Storage);

    // Default selector returns hdfs storage
    when(defaultStorageSelector.getName()).thenReturn(DefaultStorageSelector.class.getSimpleName());
    Storage hdfsStorage = mock(HdfsStorage.class);
    when(defaultStorageSelector.selectStorage(anyString(), anyString())).thenReturn(hdfsStorage);

    storageSelectorConfig.defaultStorageSelector = defaultStorageSelector;
    storageSelectorConfig.storageSelectors =
        Arrays.asList(defaultStorageSelector, regexStorageSelector);

    assertEquals(
        RegexStorageSelector.class.getSimpleName(),
        storageSelectorConfig.provideStorageSelector().getName());

    StorageSelector selector = storageSelectorConfig.provideStorageSelector();
    // s3 storage  must match
    assertEquals(s3Storage, selector.selectStorage(anyString(), anyString()));
    // hdfs storage instance must not match
    assertNotEquals(hdfsStorage, selector.selectStorage(anyString(), anyString()));
  }
}
