package com.linkedin.openhouse.cluster.storage.adls;

import com.linkedin.openhouse.cluster.storage.BaseStorage;
import com.linkedin.openhouse.cluster.storage.StorageClient;
import com.linkedin.openhouse.cluster.storage.StorageType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * The ADLS Storage class is an implementation of the Storage interface for cloud storage with Azure
 * Data Lake Storage.
 */
@Component
public class AdlsStorage extends BaseStorage {

  // Declare client class to interact with ADLS filesystems/storage
  @Autowired @Lazy private AdlsStorageClient adlsStorageClient;

  /**
   * Get the type of the ADLS storage.
   *
   * @return the type of the ADLS storage
   */
  @Override
  public StorageType.Type getType() {
    return StorageType.ADLS;
  }

  /**
   * Get the ADLS storage client.
   *
   * @return the ADLS storage client
   */
  @Override
  public StorageClient<?> getClient() {
    return adlsStorageClient;
  }
}
