package com.linkedin.openhouse.cluster.storage.selector;

/** An abstract class for all Storage selectors */
public abstract class BaseStorageSelector implements StorageSelector {

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
