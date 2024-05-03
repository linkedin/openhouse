package com.linkedin.openhouse.cluster.storage;

import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The BaseStorage class is an abstract class that implements the Storage interface. It provides
 * common functionality for all storage implementations.
 */
public abstract class BaseStorage implements Storage {

  @Autowired private StorageProperties storageProperties;

  /**
   * Check if the storage is configured.
   *
   * <p>The storage is considered configured if type is defined in the storage properties.
   *
   * @return true if the storage is configured, false otherwise
   */
  @Override
  public boolean isConfigured() {
    return Optional.ofNullable(storageProperties.getTypes())
        .map(types -> types.containsKey(getType().getValue()))
        .orElse(false);
  }

  /**
   * Get the properties of the storage.
   *
   * @return a copy of map of properties of the storage
   */
  @Override
  public Map<String, String> getProperties() {
    return Optional.ofNullable(storageProperties.getTypes())
        .map(types -> types.get(getType().getValue()))
        .map(StorageProperties.StorageTypeProperties::getParameters)
        .map(HashMap::new)
        .orElseGet(HashMap::new);
  }
}
