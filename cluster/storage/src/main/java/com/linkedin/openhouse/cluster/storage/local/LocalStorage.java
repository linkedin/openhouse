package com.linkedin.openhouse.cluster.storage.local;

import com.linkedin.openhouse.cluster.storage.BaseStorage;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import java.nio.file.Paths;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * The LocalStorage class is an implementation of the Storage interface for local storage. It uses a
 * LocalStorageClient to interact with the local file system. The LocalStorageClient uses an Apache
 * Hadoop FileSystem to interact with the local file system.
 */
@Component
public class LocalStorage extends BaseStorage {

  private static final StorageType.Type LOCAL_TYPE = StorageType.LOCAL;

  @Autowired private StorageProperties storageProperties;

  // Lazy initialization of the LocalStorageClient
  @Autowired @Lazy private LocalStorageClient localStorageClient;

  /**
   * Check if the local storage is configured.
   *
   * <p>The local storage is considered configured if the default type is not set or no types are
   * provided or specific "local" type is provided.
   *
   * @return true if the local storage is configured, false otherwise
   */
  @Override
  public boolean isConfigured() {
    if (storageProperties.getDefaultType() == null) {
      return true;
    } else if (storageProperties.getTypes() == null || storageProperties.getTypes().isEmpty()) {
      return true;
    } else {
      return storageProperties.getTypes().containsKey(LOCAL_TYPE.getValue());
    }
  }

  @Override
  public StorageType.Type getType() {
    return LOCAL_TYPE;
  }

  @Override
  public StorageClient<?> getClient() {
    return localStorageClient;
  }

  /**
   * Allocates Table Space for the Local Storage.
   *
   * <p>tableLocation looks like: /{rootPrefix}/{databaseId}/{tableId}-{tableUUID} We strip the
   * endpoint to ensure backward-compatibility. This override should be removed after resolving <a
   * href="https://github.com/linkedin/openhouse/issues/121">
   */
  @Override
  public String allocateTableLocation(
      String databaseId,
      String tableId,
      String tableUUID,
      String tableCreator,
      Map<String, String> tableProperties) {
    return Paths.get(getClient().getRootPrefix(), databaseId, tableId + "-" + tableUUID).toString();
  }
}
