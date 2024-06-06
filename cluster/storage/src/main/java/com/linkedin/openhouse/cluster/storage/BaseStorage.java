package com.linkedin.openhouse.cluster.storage;

import com.google.common.base.Preconditions;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.net.URI;
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

  /**
   * Allocates Table Space for the storage.
   *
   * <p>Default tableLocation looks like: {endpoint}/{rootPrefix}/{databaseId}/{tableId}-{tableUUID}
   *
   * @return the table location where the table data should be stored
   */
  @Override
  public String allocateTableSpace(
      String databaseId,
      String tableId,
      String tableUUID,
      String tableCreator,
      boolean skipProvisioning) {
    Preconditions.checkArgument(databaseId != null, "Database ID cannot be null");
    Preconditions.checkArgument(tableId != null, "Table ID cannot be null");
    Preconditions.checkArgument(tableUUID != null, "Table UUID cannot be null");
    Preconditions.checkState(
        storageProperties.getTypes().containsKey(getType().getValue()),
        "Storage properties doesn't contain type: " + getType().getValue());
    return URI.create(
            getClient().getEndpoint()
                + getClient().getRootPrefix()
                + "/"
                + databaseId
                + "/"
                + tableId
                + "-"
                + tableUUID)
        .normalize()
        .toString();
  }
}
