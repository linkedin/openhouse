package com.linkedin.openhouse.tables.mock.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;

@SpringBootTest
@Slf4j
public class StorageManagerTest {

  @Autowired private StorageManager storageManager;

  @MockBean private StorageProperties storageProperties;

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
  public void validatePropertiesShouldThrowExceptionWhenDefaultTypeIsNullAndTypesIsNotEmpty() {
    when(storageProperties.getDefaultType()).thenReturn(null);
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.HDFS.getValue(), new StorageProperties.StorageTypeProperties()));

    assertThrows(IllegalArgumentException.class, () -> storageManager.validateProperties());
  }

  @Test
  public void validatePropertiesShouldThrowExceptionWhenDefaultTypeIsNotNullAndTypesIsEmpty() {
    when(storageProperties.getDefaultType()).thenReturn(StorageType.HDFS.getValue());
    when(storageProperties.getTypes()).thenReturn(Collections.emptyMap());

    assertThrows(IllegalArgumentException.class, () -> storageManager.validateProperties());
  }

  @Test
  public void validatePropertiesShouldThrowExceptionWhenDefaultTypeIsNotNullAndTypeIsNotPresent() {
    when(storageProperties.getDefaultType()).thenReturn(StorageType.HDFS.getValue());
    when(storageProperties.getTypes())
        .thenReturn(ImmutableMap.of("valid", new StorageProperties.StorageTypeProperties()));

    assertThrows(IllegalArgumentException.class, () -> storageManager.validateProperties());
  }

  @Test
  public void validatePropertiesShouldThrowExceptionWhenDefaultTypeIsInvalid() {
    when(storageProperties.getDefaultType()).thenReturn("invalid");
    when(storageProperties.getTypes())
        .thenReturn(ImmutableMap.of("invalid", new StorageProperties.StorageTypeProperties()));

    assertThrows(IllegalArgumentException.class, () -> storageManager.validateProperties());
  }

  @Test
  public void validateEmptyPropertiesGivesLocalStorage() {
    when(storageProperties.getDefaultType()).thenReturn(null);
    when(storageProperties.getTypes()).thenReturn(null);

    assertDoesNotThrow(() -> storageManager.validateProperties());
    assert (storageManager.getDefaultStorage().getType().equals(StorageType.LOCAL));
    assert (storageManager.getStorage(StorageType.LOCAL).isConfigured());
  }

  @Test
  public void validatePropertiesGivesHdfsStorage() {
    when(storageProperties.getDefaultType()).thenReturn(StorageType.HDFS.getValue());
    when(storageProperties.getTypes())
        .thenReturn(
            ImmutableMap.of(
                StorageType.HDFS.getValue(), new StorageProperties.StorageTypeProperties()));

    assertDoesNotThrow(() -> storageManager.validateProperties());
  }
}
