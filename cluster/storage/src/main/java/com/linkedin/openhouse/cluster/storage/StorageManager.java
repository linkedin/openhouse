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
@Slf4j
@Component
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

  /**
   * Get the storage from the provided path. Iterate over all storages and return the storage which
   * is configured and its endpoint is the prefix of supplied path. Note: if local storage type is
   * configured, then return the local storage. This is a workaround for unit tests to work because
   * hdfs and local storage do not append scheme to the paths. See:
   * https://github.com/linkedin/openhouse/issues/121
   *
   * @param path Path that contains the scheme
   * @return the storage
   */
  public Storage getStorageFromPath(
      String databaseId, String tableId, String tableUUID, String path) {
    for (Storage storage : storages) {
      if (storage.isConfigured()) {
        if (storage.isPathValid(databaseId, tableId, tableUUID, path)) {
          log.info("Resolved to {} storage for path {}", storage.getType().toString(), path);
          return storage;
        } else if (StorageType.LOCAL.equals(storage.getType())) {
          log.info("Resolved to local storage for path {}", path);
          return storage;
        }
      }
    }

    throw new IllegalArgumentException("Unable to determine storage for path: " + path);
  }
}
