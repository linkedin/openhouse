package com.linkedin.openhouse.cluster.storage.selector;

import com.linkedin.openhouse.cluster.storage.Storage;

/**
 * The StorageSelector interface provides a way to select storage per table
 *
 * <p>Implementations of this interface can choose to return different storages for different db and
 * tables For Example {@link DefaultStorageSelector} returns the cluster level storage for all
 * tables
 */
public interface StorageSelector {
  /**
   * Select the storage for given db and table
   *
   * @param db
   * @param table
   * @return {@link Storage}
   */
  Storage selectStorage(String db, String table);

  /**
   * Get the Simple class name of the implementing class
   *
   * @return
   */
  String getName();
}
