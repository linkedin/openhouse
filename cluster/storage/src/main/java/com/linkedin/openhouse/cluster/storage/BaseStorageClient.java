package com.linkedin.openhouse.cluster.storage;

import com.google.common.base.Preconditions;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import org.springframework.util.CollectionUtils;

/**
 * BaseStorageClient is the abstract class that holds common functionality for all storage clients
 * like validating the configuration and storage properties for a given storage type.
 *
 * @param <T> native client type for the storage backend.
 */
public abstract class BaseStorageClient<T> implements StorageClient<T> {
  // Validates the configuration for the given storage type. Validates that:
  // 1. The storageProperties contain the entry for the given storage type.
  // 2. The storageProperties value for the given storage type is not null and has the endpoint and
  // root path configured.
  protected void validateProperties(
      StorageProperties storageProperties, StorageType.Type storageType) {
    Preconditions.checkArgument(
        !CollectionUtils.isEmpty(storageProperties.getTypes())
            && storageProperties.getTypes().containsKey(storageType.getValue()),
        "Storage properties doesn't contain type: " + storageType.getValue());
    StorageProperties.StorageTypeProperties hdfsStorageProperties =
        storageProperties.getTypes().get(storageType.getValue());
    Preconditions.checkArgument(
        hdfsStorageProperties != null,
        "Storage properties doesn't contain type: " + storageType.getValue());
    Preconditions.checkArgument(
        hdfsStorageProperties.getEndpoint() != null,
        "Storage properties doesn't contain endpoint for: " + storageType.getValue());
    Preconditions.checkArgument(
        hdfsStorageProperties.getRootPath() != null,
        "Storage properties doesn't contain rootpath for: " + storageType.getValue());
  }
}
