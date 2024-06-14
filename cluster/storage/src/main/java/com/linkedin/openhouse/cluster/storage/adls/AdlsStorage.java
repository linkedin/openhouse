package com.linkedin.openhouse.cluster.storage.abs;

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
public class ADLSStorage extends BaseStorage {

  @Autowired @Lazy
  private ADLSStorageClient
      adlsStorageClient; // declare client class to interact with ADLS filesystem

  // do we need an isConfigured method? to check if ADLS is configured. Leaving out for now

  @Override
  public StorageType.Type getType() {
    return StorageType.ADLS;
  }

  @Override
  public StorageClient<?> getClient() {
    return adlsStorageClient;
  }
}
