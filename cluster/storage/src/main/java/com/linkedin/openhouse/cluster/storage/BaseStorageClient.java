package com.linkedin.openhouse.cluster.storage;

import com.google.common.base.Preconditions;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

/**
 * BaseStorageClient is the abstract class that holds common functionality for all storage clients
 * like validating the configuration and storage properties for a given storage type.
 *
 * @param <T> native client type for the storage backend.
 */
@Slf4j
public abstract class BaseStorageClient<T> implements StorageClient<T> {
  // Holds storage properties for all configured storage types.
  @Autowired private StorageProperties storageProperties;

  /**
   * Returns the endpoint for the given storage type.
   *
   * @return endpoint for the given storage type.
   */
  @Override
  public String getEndpoint() {
    return storageProperties.getTypes().get(getStorageType().getValue()).getEndpoint();
  }

  /**
   * Returns the root prefix for the given storage type.
   *
   * @return root prefix for the given storage type.
   */
  @Override
  public String getRootPrefix() {
    return storageProperties.getTypes().get(getStorageType().getValue()).getRootPath();
  }

  // Validates the configuration for the given storage type.
  // The storage type is determined based on the derived class implementation of getStorageType().
  // Validates that:
  // 1. The storageProperties contain the entry for the given storage type.
  // 2. The storageProperties value for the given storage type is not null and has the endpoint and
  // root path configured.
  protected void validateProperties() {
    // Get the storage type from the derived class implementation.
    StorageType.Type storageType = getStorageType();

    // Validate that the storageProperties contains the given storage type.
    Preconditions.checkArgument(
        !CollectionUtils.isEmpty(storageProperties.getTypes())
            && storageProperties.getTypes().containsKey(storageType.getValue()),
        "Storage properties doesn't contain type: " + storageType.getValue());

    // Extract the value of storage properties for the given storage type.
    StorageProperties.StorageTypeProperties storagePropertiesForType =
        storageProperties.getTypes().get(storageType.getValue());

    // Validate that the storage properties for the given type are not null.
    Preconditions.checkArgument(
        storagePropertiesForType != null,
        "Storage properties doesn't contain type: " + storageType.getValue());

    // Validate that the endpoint is configured in the storage properties for the given type.
    Preconditions.checkArgument(
        getEndpoint() != null,
        "Storage properties doesn't contain endpoint for: " + storageType.getValue());

    // Validate that the root prefix is configured in the storage properties for the given type.
    Preconditions.checkArgument(
        getRootPrefix() != null,
        "Storage properties doesn't contain rootpath for: " + storageType.getValue());
  }
}
