package com.linkedin.openhouse.cluster.storage;

import static com.linkedin.openhouse.cluster.storage.StorageType.LOCAL;

import com.google.common.base.Preconditions;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * The StorageManager class is responsible for managing the storage types and providing the
 * appropriate storage implementation based on the configuration.
 */
@Component
@Slf4j
public class StorageManager {

  @Autowired StorageProperties storageProperties;

  @Autowired StorageType storageType;

  @Autowired List<Storage> storages;

  /**
   * Validate the storage properties.
   *
   * <p>It validates storage properties as follows:
   *
   * <p>1. If any storage type is configured, then default type must be set. Alternatively, if a
   * default type is not set, then storage types should also be null or empty. **valid**
   *
   * <p>2. If default-type is set, then the value of the default type must exist in the configured
   * storage types. **valid**
   *
   * <p>all other configurations are **invalid**
   */
  @PostConstruct
  public void validateProperties() {
    log.info("system: {}", System.getProperty("OPENHOUSE_CLUSTER_CONFIG_PATH"));
    log.info("postconstruct: {}", this);
    log.info("prop: {}", storageProperties);
    log.info("defaultType: {}", storageProperties.getDefaultType());
    String clusterYamlError = "Cluster yaml is incorrectly configured: ";
    if (StringUtils.hasText(storageProperties.getDefaultType())) {
      // default-type is configured, types should contain the default-type
      Preconditions.checkArgument(
          !CollectionUtils.isEmpty(storageProperties.getTypes())
              && storageProperties.getTypes().containsKey(storageProperties.getDefaultType()),
          clusterYamlError
              + "storage types should contain the default-type: "
              + storageProperties.getDefaultType());
    } else {
      // default-type is not configured, types should be null or empty
      Preconditions.checkArgument(
          CollectionUtils.isEmpty(storageProperties.getTypes()),
          clusterYamlError + "default-type must be set if storage types are configured");
    }
    try {
      Optional.ofNullable(storageProperties.getDefaultType()).ifPresent(storageType::fromString);
      Optional.ofNullable(storageProperties.getTypes())
          .map(Map::keySet)
          .ifPresent(keyset -> keyset.forEach(key -> storageType.fromString(key)));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(clusterYamlError + e.getMessage());
    }
  }

  /**
   * Get the default storage.
   *
   * @return the default storage
   */
  public Storage getDefaultStorage() {
    if (!StringUtils.hasText(storageProperties.getDefaultType())) {
      return getStorage(LOCAL);
    }
    return getStorage(storageType.fromString(storageProperties.getDefaultType()));
  }

  /**
   * Get the storage based on the storage type.
   *
   * @param storageType the storage type
   * @return the storage
   */
  public Storage getStorage(StorageType.Type storageType) {
    for (Storage storage : storages) {
      if (storage.getType().equals(storageType) && storage.isConfigured()) {
        return storage;
      }
    }
    throw new IllegalArgumentException(
        "No configured storage found for type: " + storageType.getValue());
  }
}
