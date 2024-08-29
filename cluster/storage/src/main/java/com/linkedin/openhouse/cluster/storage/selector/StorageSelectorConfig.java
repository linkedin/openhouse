package com.linkedin.openhouse.cluster.storage.selector;

import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures the StorageSelector bean for storage-selector configured in {@link StorageProperties}
 * The return value of the bean is the {@link StorageSelector} implementation that matches the name
 * in storage-selector or is null if the storage selector is not configured.
 */
@Slf4j
@Configuration
public class StorageSelectorConfig {

  @Autowired StorageProperties storageProperties;

  @Autowired List<StorageSelector> storageSelectors;

  /**
   * Checks the name of storage-selector from {@link StorageProperties} against all implementations
   * of {@link StorageSelector} and returns the implementation that matches the name. returns null
   * if not configured
   *
   * @return
   */
  @Bean("StorageSelector")
  StorageSelector provideStorageSelector() {
    String selectorName;
    try {
      selectorName = storageProperties.getStorageSelector().getName();
      for (StorageSelector selector : storageSelectors) {
        if (selectorName.equals(selector.getName())) {
          return selector;
        }
      }
    } catch (Exception e) {
      // if storage selector is not configured. Return null.
      // Spring doesn't define the bean if the return value is null
      log.error("Exception initializing Storage selector.");
      return null;
    }

    throw new IllegalArgumentException("Could not find Storage selector with name=" + selectorName);
  }
}
