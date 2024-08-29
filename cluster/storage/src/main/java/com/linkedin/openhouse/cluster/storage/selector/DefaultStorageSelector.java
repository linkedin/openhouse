package com.linkedin.openhouse.cluster.storage.selector;

import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * An implementation of {@link StorageSelector} that returns cluster level storage for all tables
 */
@Component
public class DefaultStorageSelector extends BaseStorageSelector {

  @Autowired private StorageManager storageManager;

  /**
   * Get the cluster level storage for all tables
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
