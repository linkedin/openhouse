package com.linkedin.openhouse.cluster.storage.selector;

import com.linkedin.openhouse.cluster.storage.configs.StorageProperties;
import org.springframework.beans.factory.annotation.Autowired;

/** An abstract class for all Storage selectors */
public abstract class BaseStorageSelector implements StorageSelector {

  @Autowired private StorageProperties storageProperties;

  @Override
  public String getName() {
    return storageProperties.getStorageSelector().getName();
  }
}
