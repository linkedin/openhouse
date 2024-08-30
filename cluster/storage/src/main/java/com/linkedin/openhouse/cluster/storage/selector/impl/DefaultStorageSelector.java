package com.linkedin.openhouse.cluster.storage.selector.impl;

import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.cluster.storage.selector.BaseStorageSelector;
import com.linkedin.openhouse.cluster.storage.selector.StorageSelector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * An implementation of {@link StorageSelector} that returns storage that's marked as default-type
 * for the cluster in yaml configuration for all tables
 */
@Component
public class DefaultStorageSelector extends BaseStorageSelector {

  @Autowired private StorageManager storageManager;

  /**
   * Get default-type storage for all tables
   *
   * @param db
   * @param table
   * @return {@link Storage}
   */
  @Override
  public Storage selectStorage(String db, String table) {
    return storageManager.getDefaultStorage();
  }
}
