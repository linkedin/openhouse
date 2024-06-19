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

  // Declare client class to interact with ADLS filesystem
  @Autowired @Lazy private AdlsStorageClient adlsStorageClient;

  @Override
  public StorageType.Type getType() {
    return StorageType.ADLS;
  }

  @Override
  public StorageClient<?> getClient() {
    return adlsStorageClient;
  }
}
